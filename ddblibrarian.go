/*
	Copyright (C) 2017  Marco Almeida <marcoafalmeida@gmail.com>

	This file is part of ddblibrarian.

	ddblibrarian is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation; either version 2 of the License, or
	(at your option) any later version.

	ddblibrarian is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License along
	with this program; if not, write to the Free Software Foundation, Inc.,
	51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

// Package ddblibrarian extends the Go SDK for DynamoDB by adding support for snapshots (or item versioning) in a
// fully managed way.
//
// It can be used with any existing, arbitrary, DynamoDB tables as long as the type of the partition key is either a
// string or a number.
package ddblibrarian

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const snapshotDelimiter = "."

// Represents one instance of ddblibrarian for a given DynamoDB table.
type Library struct {
	svc              *dynamodb.DynamoDB
	tableName        string
	partitionKey     string
	partitionKeyType string
	rangeKey         string
	rangeKeyType     string
	currentSnapshot  string
	// flag a Browse action;
	// we can't use currentSnapshot="" to flag it because an empty string
	// denotes pre-snapshot data, which we may want to roll back to
	browsing bool
	// cache
	// cache.set(key, value), cache.get(key), cache.invalidate(key), cache.ttl(X)
}

// New creates a new Library instance for the specified table.
//
// Each Library instance needs the primary key schema: partitionKey, partitionKeyType, rangeKey, and
// rangeKeyType. In the case of a simple primary key, i.e., only a partition key, rangeKey and rangeKeyType should be
// empty strings.
//
// The value of partitionKeyType and rangeKeyType and must be either "N" or "S".
//
// Every Library instance includes a DynamoDB client. It is created using the session for AWS services p, and,
// optionally, additional configuration details as provided by cfg.
func New(
	table string,
	partitionKey string,
	partitionKeyType string,
	rangeKey string,
	rangeKeyType string,
	p client.ConfigProvider,
	cfg ...*aws.Config,
) (*Library, error) {
	if partitionKeyType != "S" && partitionKeyType != "N" {
		return nil, errors.New("invalid key (partition or range) type: must be one of 'N' or 'S'")
	}

	return &Library{
		tableName:        table,
		partitionKey:     partitionKey,
		partitionKeyType: partitionKeyType,
		rangeKey:         rangeKey,
		rangeKeyType:     rangeKeyType,
		browsing:         false,
		svc:              dynamodb.New(p, cfg...),
	}, nil
}

// Snapshot starts a new snapshot and sets it as the active one.
//
// The snapshot will be used to store a point in time copy of each individual item written to it while it is active.
//
// Cost: 1RU + 1WU
func (c *Library) Snapshot(snapshot string) error {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return errors.New("failed to create metadata client: " + err.Error())
	}

	// TODO: naming restrictions
	_, err = meta.snapshot(snapshot)
	if err != nil {
		return errors.New("failed to create snapshot: " + err.Error())
	}

	return nil
}

// Browse sets snapshot as the active snapshot for the session currently handled by Library.
//
// Other clients, with either new or already established connections, will not be affected.
//
// Cost: 1RU
func (c *Library) Browse(snapshot string) error {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return err
	}

	current, err := meta.getSnapshotID(snapshot)
	if err != nil {
		return err
	}

	c.browsing = true
	c.currentSnapshot = current

	return nil
}

// StopBrowsing reverts the active snapshot to the one set in table's metadata.
//
// This affects the current session. Other clients, with either new or already established connections, will not be
// affected.
//
// Cost: 0
func (c *Library) StopBrowsing() {
	c.browsing = false
	c.currentSnapshot = ""
}

// Rollback sets snapshot as the active snapshot.
//
// This operation will affect all clients, both new and already established connections.
//
// Cost: 1RU + 1WU
func (c *Library) Rollback(snapshot string) error {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return err
	}

	_, err = meta.rollback(snapshot)
	if err != nil {
		return err
	}

	// if we were browsing some snapshot, we're not anymore
	c.StopBrowsing()

	return nil
}

func (c *Library) DestroySnapshot(snapshot string) {
	// TODO: remove the snapshot from the cache
}

// ListSnapshots returns a (chronological sorted) list of all existing snapshots.
//
// Cost: 1RU
func (c *Library) ListSnapshots() ([]string, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	return meta.listSnapshots(), nil
}

// PutItem calls the PutItem API operation for input. The data is written to the active snapshot.
//
// Overhead: 1RU
func (c *Library) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	var snapshotID string
	var err error

	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, errors.New("failed to create snapshots client: " + err.Error())
	}

	snapshotID, err = meta.getSnapshotID(snapshotCurrent)
	if err != nil {
		return nil, errors.New("failed to get snapshot ID: " + err.Error())
	}

	// save the key as the user passed it and add the snapshot ID
	originalKey := c.addSnapshotToPartitionKey(snapshotID, input.Item[c.partitionKey])
	// update DDB
	output, err := c.svc.PutItem(input)
	// restore the original key
	c.restorePartitionKey(originalKey, input.Item[c.partitionKey])

	return output, err
}

// BatchWriteItem wraps the BatchWriteItem API operation for Amazon DynamoDB
// (https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.BatchWriteItem).
//
// It puts or deletes multiple items in the managed table. The data is written to the active snapshot.
//
// Writing to more than one table is not supported. If any tables other than the one passed to New are
// used, the operation is aborted and an error is returned.
//
// Overhead: 1RU
func (c *Library) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	var snapshotID string
	var err error

	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, errors.New("failed to create snapshots client: " + err.Error())
	}

	// make sure we're only writing to the managed table
	if len(input.RequestItems) > 1 {
		return nil, errors.New("BatchWriteItem does not support writing data to multiple tables")
	}

	requests, ok := input.RequestItems[c.tableName]
	if !ok {
		return nil, errors.New("BathWriteItem can only write items to the managed table: " + c.tableName)
	}

	snapshotID, err = meta.getSnapshotID(snapshotCurrent)
	if err != nil {
		return nil, errors.New("failed to get snapshot ID: " + err.Error())
	}

	// add the snapshot ID to each request
	for _, r := range requests {
		if r.DeleteRequest != nil {
			c.addSnapshotToPartitionKey(snapshotID, r.DeleteRequest.Key[c.partitionKey])
		}
		if r.PutRequest != nil {
			c.addSnapshotToPartitionKey(snapshotID, r.PutRequest.Item[c.partitionKey])
		}
	}
	// update DDB
	output, err := c.svc.BatchWriteItem(input)
	// remove the snapshot ID info from the PK of requests that were not processed
	unprocessed, ok := output.UnprocessedItems[c.tableName]
	if ok {
		for _, r := range unprocessed {
			if r.DeleteRequest != nil {
				c.removeSnapshotFromPartitionKey(r.DeleteRequest.Key[c.partitionKey])
			}
			if r.PutRequest != nil {
				c.removeSnapshotFromPartitionKey(r.PutRequest.Item[c.partitionKey])
			}
		}
	}

	return output, err
}

// UpdateItem calls the UpdateItem API operation for input. The data is written to the active
// snapshot.
//
// Overhead: 1RU
func (c *Library) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	var snapshotID string
	var err error

	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, errors.New("Failed to create snapshots client: " + err.Error())
	}

	snapshotID, err = meta.getSnapshotID(snapshotCurrent)
	if err != nil {
		return nil, errors.New("Failed to get snapshot ID: " + err.Error())
	}

	// save the key as the user passed it and add the snapshot ID
	originalKey := c.addSnapshotToPartitionKey(snapshotID, input.Key[c.partitionKey])
	// update the table
	output, err := c.svc.UpdateItem(input)
	// restore the original PK value
	c.restorePartitionKey(originalKey, input.Key[c.partitionKey])

	return output, err
}

// GetItem calls the GetItem API operation on input.
//
// It will start by trying to get the item input from the active snapshot. If the item is not found, GetItem will
// try to get it from all previous snapshots, one at a time, in chronological order, until it is found.
//
// Overhead: (1+N) RU (worst case, where N is the number of snapshots)
func (c *Library) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	// default to fetching data from the active/current snapshot (could be latest or a rollback)
	startFrom := meta.getCurrentSnapshotID()
	// override in case we're browsing some specific snapshot
	if c.browsing {
		startFrom = c.currentSnapshot
	}

	snapshotIDs := meta.GetChronologicalSnapshotIDs(startFrom)

	for _, id := range snapshotIDs {
		item, err := c.getItemWithSnapshotID(input, id)
		if err != nil {
			return nil, err
		}
		if item.Item != nil {
			return item, nil
		}
	}

	// maybe the item was created before any snapshots were created
	return c.getItemWithSnapshotID(input, "")
}

// GetItemFromSnapshot calls the GetItem API operation on input. The item will be read (if it exists) from snapshot.
//
// Overhead: 1RU
func (c *Library) GetItemFromSnapshot(input *dynamodb.GetItemInput, snapshot string) (*dynamodb.GetItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.getSnapshotID(snapshot)
	if err != nil {
		return nil, err
	}

	return c.getItemWithSnapshotID(input, id)
}

func (c *Library) getItemWithSnapshotID(input *dynamodb.GetItemInput, id string) (*dynamodb.GetItemOutput, error) {
	// save the key as the user passed it and add the snapshot ID before calling GetItem
	originalKey := c.addSnapshotToPartitionKey(id, input.Key[c.partitionKey])
	//
	item, err := c.svc.GetItem(input)
	// restore the PK value
	c.restorePartitionKey(originalKey, input.Key[c.partitionKey])

	if err != nil {
		return nil, err
	}

	// remove the id information from the PK (if an item for the snapshot was found)
	_, ok := item.Item[c.partitionKey]
	if ok {
		c.restorePartitionKey(originalKey, item.Item[c.partitionKey])
	}

	return item, err
}

// BatchGetItem wraps the BatchGetItem API operation for Amazon DynamoDB
// (https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.BatchGetItem).
//
// It retrieves the attributes of one or more items from, identified by primary key.
//
// It will start by trying to get input from the active snapshot. If not found, BatchGetItem will
// try to retrieve it from all previous snapshots, one at a time, in chronological order.
//
// Retrieving items from more than one table is not supported. If any tables other than the one passed to New are
// used, the operation is aborted and an error is returned.
//
// Overhead: 1RU
func (c *Library) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	// default to fetching data from the active/current snapshot (could be latest or a rollback)
	startFrom := meta.getCurrentSnapshotID()
	// override in case we're browsing some specific snapshot
	if c.browsing {
		startFrom = c.currentSnapshot
	}

	snapshotIDs := meta.GetChronologicalSnapshotIDs(startFrom)

	for _, id := range snapshotIDs {
		output, err := c.batchGetItemWithSnapshotID(input, id)
		if err != nil {
			return nil, err
		}
		if output.Responses != nil {
			return output, nil
		}
	}

	// maybe the item was created before any snapshots were created
	return c.batchGetItemWithSnapshotID(input, "")
}

// BatchGetItemFromSnapshot retrieves the attributes of one or more items from a specific snapshot.
//
// Overhead: 1RU
func (c *Library) BatchGetItemFromSnapshot(
	input *dynamodb.BatchGetItemInput,
	snapshot string,
) (*dynamodb.BatchGetItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.getSnapshotID(snapshot)
	if err != nil {
		return nil, err
	}

	return c.batchGetItemWithSnapshotID(input, id)
}

func (c *Library) batchGetItemWithSnapshotID(
	input *dynamodb.BatchGetItemInput,
	id string,
) (*dynamodb.BatchGetItemOutput, error) {
	if len(input.RequestItems) > 1 {
		return nil, errors.New("BatchGetItem does not support retrieving data from multiple tables")
	}

	keysAndAttributes, ok := input.RequestItems[c.tableName]
	if !ok {
		return nil, errors.New("BathGetItem can only retrieve items from the managed table: " + c.tableName)
	}

	// add the snapshot ID
	for _, k := range keysAndAttributes.Keys {
		c.addSnapshotToPartitionKey(id, k[c.partitionKey])
	}
	// retrieve items
	output, err := c.svc.BatchGetItem(input)
	// restore the PK value to the variable we received
	for _, k := range keysAndAttributes.Keys {
		c.removeSnapshotFromPartitionKey(k[c.partitionKey])
	}

	if err != nil {
		return nil, err
	}

	// remove the snapshot id from the PKs retrieved
	attrs, ok := output.Responses[c.tableName]
	if ok {
		for _, k := range attrs {
			c.removeSnapshotFromPartitionKey(k[c.partitionKey])
		}
	}
	// remove the snapshot id from keys that have not been processed
	keysAndAttributes, ok = output.UnprocessedKeys[c.tableName]
	if ok {
		for _, k := range keysAndAttributes.Keys {
			c.removeSnapshotFromPartitionKey(k[c.partitionKey])
		}
	}

	return output, err
}

// Scan wraps the Scan API operation for Amazon DynamoDB
// (https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.Scan).
//
// It returns one or more items by accessing every item in a table or a secondary index and filtering by the active
// snapshot.
//
// If no snapshots exist, no filtering based on snapshots will be performed.
//
// Warning: this operation will read the whole table and filter out items that do not match the active snapshot
// before returning the data.
//
// Overhead: 1RU
func (c *Library) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	// default to fetching data from the active/current snapshot (could be latest or a rollback)
	currentSnapshotID := meta.getCurrentSnapshotID()
	// override in case we're browsing some specific snapshot
	if c.browsing {
		currentSnapshotID = c.currentSnapshot
	}

	return c.scanWithSnapshotID(input, currentSnapshotID)
}

// ScanFromSnapshot returns one or more items by accessing every item in a table or a secondary index and filtering the
// ones on the specified snapshot.
//
// If snapshot is an empty string, items from all available snapshots will be returned.
//
// If input includes the partition key in ExpressionAttributeValues, it *must* be named ":pk".
//
// Warning: this operation will read the whole table and filter out items that do not match the specified snapshot
// before returning the data.
//
// Overhead: 1RU
func (c *Library) ScanFromSnapshot(input *dynamodb.ScanInput, snapshot string) (*dynamodb.ScanOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.getSnapshotID(snapshot)
	if err != nil {
		return nil, err
	}

	return c.scanWithSnapshotID(input, id)
}

func (c *Library) scanWithSnapshotID(input *dynamodb.ScanInput, id string) (*dynamodb.ScanOutput, error) {
	// don't destroy the user provided input (unlike other cases, undoing changes here is tricky so we just make
	// a copy)
	inputCopy := *input
	// make sure the map is not nil before assigning a new value
	if inputCopy.ExpressionAttributeValues == nil {
		inputCopy.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue, 0)
	} else {
		// add the snapshot ID to the partition key
		_, ok := inputCopy.ExpressionAttributeValues[":pk"]
		if ok {
			c.addSnapshotToPartitionKey(id, inputCopy.ExpressionAttributeValues[":pk"])
		}
	}
	// we always need to filter out the row used to store our metadata
	if c.partitionKeyType == "S" {
		inputCopy.ExpressionAttributeValues[":metaPK"] = &dynamodb.AttributeValue{
			S: aws.String(ddbPartitionKey),
		}
	} else {
		inputCopy.ExpressionAttributeValues[":metaPK"] = &dynamodb.AttributeValue{
			N: aws.String(ddbPartitionKey),
		}
	}
	filterStr := fmt.Sprintf("%s <> :metaPK", c.partitionKey)
	// if no snapshot was specified, there's no need for further filtering
	if id != "" {
		// different data types require different approaches to filtering
		if c.partitionKeyType == "S" {
			inputCopy.ExpressionAttributeValues[":prefix"] = &dynamodb.AttributeValue{
				S: aws.String(getSnapshotPrefix(id)),
			}
			filterStr += fmt.Sprintf(" AND begins_with(%s, :prefix)", c.partitionKey)
		} else {
			idInt, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				return nil, errors.New("failed to convert snapshot ID to integer: " + err.Error())
			}
			inputCopy.ExpressionAttributeValues[":currentID"] = &dynamodb.AttributeValue{
				N: aws.String(id),
			}
			inputCopy.ExpressionAttributeValues[":nextID"] = &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(int(idInt + 1))),
			}
			filterStr += fmt.Sprintf(
				" AND %s >= :currentID AND %s < :nextID",
				c.partitionKey,
				c.partitionKey,
			)
		}
	}

	// make sure the FilterExpression has been initialized and is ready for us to concatenate the rule that
	// filters a snapshot
	if input.FilterExpression == nil {
		inputCopy.FilterExpression = aws.String(filterStr)
	} else {
		inputCopy.FilterExpression = aws.String(*inputCopy.FilterExpression + " AND " + filterStr)
	}

	out, err := c.svc.Scan(&inputCopy)
	if err != nil {
		return nil, err
	}

	// remove the snapshot id from keys that have not been processed
	for _, item := range out.Items {
		c.removeSnapshotFromPartitionKey(item[c.partitionKey])
	}

	return out, err
}

// DeleteItem calls the DeleteItem API operation on input.
//
// It will start by trying to delete the item input from the active snapshot. If the item is not found, DeleteItem will
// try to delete it from all previous snapshots, one at a time, in chronological order, until it is found.
//
// Overhead: (1+N) RU (worst case, where N is the number of snapshots)
func (c *Library) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	// default to fetching data from the active/current snapshot (could be latest or a rollback)
	startFrom := meta.getCurrentSnapshotID()
	// override in case we're browsing some specific snapshot
	if c.browsing {
		startFrom = c.currentSnapshot
	}

	snapshotIDs := meta.GetChronologicalSnapshotIDs(startFrom)

	// we need this to know whether or not something was deleted (and therefore stop and return)
	// or nothing was found (and we need to try the previous snapshot)
	input.ReturnValues = aws.String("ALL_OLD")
	for _, id := range snapshotIDs {
		output, err := c.deleteItemWithSnapshotID(input, id)
		if err == nil {
			if output.Attributes != nil {
				return output, nil
			}
		}
	}

	// maybe the item was created before any snapshots existed
	return c.deleteItemWithSnapshotID(input, "")
}

// DeleteItemFromSnapshot calls the DeleteItem API operation on input. The item will be deleted (if it exists) from
// snapshot.
//
// Overhead: 1RU
func (c *Library) DeleteItemFromSnapshot(input *dynamodb.DeleteItemInput, snapshot string) (*dynamodb.DeleteItemOutput, error) {
	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.getSnapshotID(snapshot)
	if err != nil {
		return nil, err
	}

	// we need this to know whether or not something was deleted (and therefore stop and return)
	// or nothing was found (and we need to try the previous snapshot)
	input.ReturnValues = aws.String("ALL_OLD")

	return c.deleteItemWithSnapshotID(input, id)
}

func (c *Library) deleteItemWithSnapshotID(input *dynamodb.DeleteItemInput, id string) (*dynamodb.DeleteItemOutput, error) {
	// save the key as the user passed it and add the snapshot ID before calling DeleteItem
	originalKey := c.addSnapshotToPartitionKey(id, input.Key[c.partitionKey])
	//
	output, err := c.svc.DeleteItem(input)
	// restore the PK value
	c.restorePartitionKey(originalKey, input.Key[c.partitionKey])

	return output, err
}

// add a snapshot ID to the partition key of a given attribute
func (c *Library) addSnapshotToPartitionKey(snapshotID string, pk *dynamodb.AttributeValue) string {
	// extract the value of the partition key (depends on the type)
	originalKey := ""
	if c.partitionKeyType == "S" {
		originalKey = *pk.S
	} else {
		originalKey = *pk.N
	}

	// just skip it if there's no snapshot ID
	if snapshotID == "" {
		return originalKey
	}

	// create the new partition key which include the snapshot and update the attribute
	snapshotKey := fmt.Sprintf("%s%s", getSnapshotPrefix(snapshotID), originalKey)
	if c.partitionKeyType == "S" {
		pk.SetS(snapshotKey)
	} else {
		pk.SetN(snapshotKey)
	}

	return originalKey
}

// restore an (arbitrary) value to the partition key of a given attribute
func (c *Library) restorePartitionKey(original string, pk *dynamodb.AttributeValue) {
	if c.partitionKeyType == "S" {
		pk.SetS(original)
	} else {
		pk.SetN(original)
	}
}

// remove the snapshot ID from a partition key by finding the delimiter and removing everything to its left
func (c *Library) removeSnapshotFromPartitionKey(pk *dynamodb.AttributeValue) {
	var keyWithSnapshot *string

	if c.partitionKeyType == "S" {
		keyWithSnapshot = pk.S
	} else {
		keyWithSnapshot = pk.N
	}

	i := strings.Index(*keyWithSnapshot, snapshotDelimiter)
	if i != -1 {
		key := (*keyWithSnapshot)[i+1:]
		if c.partitionKeyType == "S" {
			pk.SetS(key)
		} else {
			pk.SetN(key)
		}
	}
}

func getSnapshotPrefix(snapshotID string) string {
	return fmt.Sprintf("%s%s", snapshotID, snapshotDelimiter)
}
