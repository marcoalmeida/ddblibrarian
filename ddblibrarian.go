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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
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

// BatchGetItem
//
// Retrieving items from more than one table is not supported. If any tables other than the one passed to New are
// used, the operation is aborted and an error is returned.
//
// Overhead: 1RU
//func (c *Library) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
//	meta, err := newMeta(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
//	if err != nil {
//		return nil, err
//	}
//
//	// default to fetching data from the active/current snapshot (could be latest or a rollback)
//	startFrom := meta.getCurrentSnapshotID()
//	// override in case we're browsing some specific snapshot
//	if c.browsing {
//		startFrom = c.currentSnapshot
//	}
//
//	snapshotIDs := meta.GetChronologicalSnapshotIDs(startFrom)
//
//	for _, id := range snapshotIDs {
//		item, err := c.batchGetItemWithSnapshotID(input, id)
//		if err != nil {
//			return nil, err
//		}
//		if item.Item != nil {
//			return item, nil
//		}
//	}
//
//	// maybe the item was created before any snapshots were created
//	return c.getItemWithSnapshotID(input, "")
//}

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

//func (c *Library) batchGetItemWithSnapshotID(
//	input *dynamodb.BatchGetItemInput,
//	id string,
//) (*dynamodb.BatchGetItemOutput, error) {
//	if len(input.RequestItems) != 1 {
//		return nil, errors.New("BatchGetItem does not support retrieving data from multiple tables")
//	}
//
//	keysAndAttributes, ok := input.RequestItems[c.tableName]
//	if !ok {
//		return nil, errors.New("BathGetItem can only retrieve items from the managed table: " + c.tableName)
//	}
//
//	// add the snapshot ID
//	for _, k := range keysAndAttributes.Keys {
//		c.addSnapshotToPartitionKey(id, k[c.partitionKey])
//	}
//	// retrieve items
//	item, err := c.svc.BatchGetItem(input)
//	// restore the PK value to the variable we received
//	for _, k := range keysAndAttributes.Keys {
//		c.addSnapshotToPartitionKey(id, k[c.partitionKey])
//	}
//
//	if err != nil {
//		return nil, err
//	}
//
//	// remove the id information from the PK (if an item for the snapshot was found)
//	_, ok := item.Item[c.partitionKey]
//	if ok {
//		c.restorePartitionKey(originalKey, item.Item[c.partitionKey])
//	}
//
//	return item, err
//}

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
func (c *Library) addSnapshotToPartitionKey(id string, pk *dynamodb.AttributeValue) string {
	// extract the value of the partition key (depends on the type)
	originalKey := ""
	if c.partitionKeyType == "S" {
		originalKey = *pk.S
	} else {
		originalKey = *pk.N
	}

	// create the new partition key which include the snapshot and update the attribute
	snapshotKey := fmt.Sprintf("%s%s%s", id, snapshotDelimiter, originalKey)
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
	var key string

	if c.partitionKeyType == "S" {
		keyWithSnapshot = pk.S
	} else {
		keyWithSnapshot = pk.N
	}

	key = *keyWithSnapshot
	i := strings.Index(*keyWithSnapshot, snapshotDelimiter)
	if i != -1 {
		key = (*keyWithSnapshot)[i+1:]
	}

	if c.partitionKeyType == "S" {
		pk.SetS(key)
	} else {
		pk.SetN(key)
	}
}
