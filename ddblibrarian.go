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


// Package ddblibrarian implements thin wrapper around the Go SDK for DynamoDB to add support for item versioning
package ddblibrarian

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/marcoalmeida/ddblibrarian/self"
)

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

func New(
	tableName string,
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
		tableName:        tableName,
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

	// save the PK value that we're about to change
	savedKey := c.getPKValueWithType(input.Item[c.partitionKey])
	// prepend the snapshot ID (depends on type)
	if c.partitionKeyType == "S" {
		input.Item[c.partitionKey].SetS(snapshotID + savedKey)
	} else {
		input.Item[c.partitionKey].SetN(snapshotID + savedKey)
	}

	// update DDB
	output, err := c.svc.PutItem(input)

	// restore the original PK value
	if c.partitionKeyType == "S" {
		input.Item[c.partitionKey].SetS(savedKey)
	} else {
		input.Item[c.partitionKey].SetN(savedKey)
	}

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

	// save the PK value that we're about to change
	savedKey := c.getPKValueWithType(input.Key[c.partitionKey])
	// prepend the snapshot ID (depends on type)
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(snapshotID + savedKey)
	} else {
		input.Key[c.partitionKey].SetN(snapshotID + savedKey)
	}

	// update the table
	output, err := c.svc.UpdateItem(input)

	// restore the original PK value
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(savedKey)
	} else {
		input.Key[c.partitionKey].SetN(savedKey)
	}

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
	// we're about to touch a field on a struct passed by the caller, let's save it to restore
	savedKey := c.getPKValueWithType(input.Key[c.partitionKey])

	// add the id to the PK before calling GetItem
	searchKey := id + savedKey
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(searchKey)
	} else {
		input.Key[c.partitionKey].SetN(searchKey)
	}

	item, err := c.svc.GetItem(input)

	// restore the PK value
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(savedKey)
	} else {
		input.Key[c.partitionKey].SetN(savedKey)
	}

	if err != nil {
		return nil, err
	}

	// remove the id information from the PK (if an item for the snapshot was found)
	_, ok := item.Item[c.partitionKey]
	if ok {
		if c.partitionKeyType == "S" {
			item.Item[c.partitionKey].SetS(savedKey)
		} else {
			item.Item[c.partitionKey].SetN(savedKey)
		}
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
	// we're about to touch a field on a struct passed by the caller, let's save it to restore
	savedKey := c.getPKValueWithType(input.Key[c.partitionKey])

	// add the id to the PK before calling GetItem
	searchKey := id + savedKey
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(searchKey)
	} else {
		input.Key[c.partitionKey].SetN(searchKey)
	}

	output, err := c.svc.DeleteItem(input)

	// restore the PK value
	if c.partitionKeyType == "S" {
		input.Key[c.partitionKey].SetS(savedKey)
	} else {
		input.Key[c.partitionKey].SetN(savedKey)
	}

	return output, err
}

// extract the value of the PK according to the type
func (c *Library) getPKValueWithType(attribute *dynamodb.AttributeValue) string {
	if c.partitionKeyType == "S" {
		return *attribute.S
	} else {
		return *attribute.N
	}
}
