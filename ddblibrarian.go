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
	"github.com/marcoalmeida/ddblibrarian/self"
)

const snapshotDelimiter = "."

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
//
// partitionKey: partition key attribute
// rangeKey: range key attribute, empty string if one does not exist
// partitionKeyType and rangeKeyType:
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

// Snapshot creates a new point in time copy of individual items. An item exists in the snapshot to which
// it was written and all future ones.
//
// Cost: 1RU + 1WU
func (c *Library) Snapshot(snapshot string) error {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return errors.New("failed to create metadata client: " + err.Error())
	}

	// TODO: naming restrictions
	_, err = meta.Snapshot(snapshot)
	if err != nil {
		return errors.New("failed to create snapshot: " + err.Error())
	}

	return nil
}

// Browse changes the active snapshot for this session only.
// Other clients' sessions, including active ones, will not be affected.
//
// Cost: 1RU
func (c *Library) Browse(snapshot string) error {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return err
	}

	current, err := meta.GetSnapshotID(snapshot)
	if err != nil {
		return err
	}

	c.browsing = true
	c.currentSnapshot = current

	return nil
}

// StopBrowsing reverts the active snapshot to whatever the current state of the table is.
//
// Cost: 0
func (c *Library) StopBrowsing() {
	c.browsing = false
	c.currentSnapshot = ""
}

// Rollback changes the active snapshot and reverts the DynamoDB table to its state at the time the snapshot was taken.
//
// Cost: 1RU + 1WU
func (c *Library) Rollback(snapshot string) error {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return err
	}

	_, err = meta.Rollback(snapshot)
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

func (c *Library) ListSnapshots() ([]string, error) {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	return meta.ListSnapshots(), nil
}

// Cost: 1RU + 1WU
func (c *Library) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	// TODO: potential optimization: cache the latest version locally and update every X seconds
	// TODO: leads to loss of accuracy but may significantly reduce the number of queries executed
	// TODO: on systems with high write traffic
	var snapshotID string
	var err error

	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, errors.New("failed to create snapshots client: " + err.Error())
	}

	snapshotID, err = meta.GetSnapshotID(self.SNAPSHOT_CURRENT)
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

// UpdateItem edits an existing item's attributes, or adds a new item to the table if it does not already exist,
// on the active snapshot.
//
// Overhead: 1 read unit
func (c *Library) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	var snapshotID string
	var err error

	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, errors.New("Failed to create snapshots client: " + err.Error())
	}

	snapshotID, err = meta.GetSnapshotID(self.SNAPSHOT_CURRENT)
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

// Cost: (1 + N)RU -- worst case, where N is the number of snapshots
func (c *Library) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	// default to fetching data from the active/current snapshot (could be latest or a rollback)
	startFrom := meta.GetCurrentSnapshotID()
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

// Cost: 2RU
func (c *Library) GetItemFromSnapshot(input *dynamodb.GetItemInput, snapshot string) (*dynamodb.GetItemOutput, error) {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.GetSnapshotID(snapshot)
	if err != nil {
		return nil, err
	}

	return c.getItemWithSnapshotID(input, id)
}

// Cost: 1RU
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

// DeleteItem deletes `input` from the most recent snapshot where it exists
func (c *Library) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	startFrom := self.SNAPSHOT_LATEST
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

func (c *Library) DeleteItemFromSnapshot(input *dynamodb.DeleteItemInput, snapshot string) (*dynamodb.DeleteItemOutput, error) {
	meta, err := self.New(c.svc, c.tableName, c.partitionKey, c.partitionKeyType, c.rangeKey, c.rangeKeyType)
	if err != nil {
		return nil, err
	}

	id, err := meta.GetSnapshotID(snapshot)
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
