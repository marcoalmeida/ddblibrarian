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

package self

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	// we need a way to identify the meta-data (stored in the target table)
	// a number will work with both data types we support (S and N)
	// this is a UUID minus the last digit -- unique enough?
	ddbPartitionKey = "10998317287113653723324905557015239445"
	// a range key is totally irrelevant to us, but one most exist if
	// the table we're working with has a range key
	ddbRangeKey = "23924679894624777035069814726213883132"
	// map snapshot_name -> snapshot_id
	ddbSnapshotsField = "snapshots"
	// ordered list of snapshot IDs -- not sequential integers!
	ddbOrderedIDs = "ids_list"
	// last snapshot to be taken
	ddbLatestIDField = "latest_snapshot"
	// snapshot to read/write from/to -- usually the most recent one
	// but will change after a rollback
	ddbCurrentIDField = "current_snapshot"
	// number of digits to use for snapshot IDs
	snapshotIDLength = 2
)

// just to make it nicer for other packages to call this one
// strings instead of integers because we need them to be of
// same type as snapshots
const (
	SNAPSHOT_LATEST  = "latest"
	SNAPSHOT_CURRENT = "current"
)

type config struct {
	svc                      *dynamodb.DynamoDB
	tableName                string
	partitionKey             string
	partitionKeyType         string
	rangeKey                 string
	rangeKeyType             string
	metaPrimaryKey           map[string]*dynamodb.AttributeValue
	snapshots                map[string]*dynamodb.AttributeValue
	chronologicalSnapshotIDs []string
	currentSnapshotID        string
	latestSnapshotID         string
}

// New creates a new instance for querying and managing snapshot-related metadata.
// It caches data locally. If consistency is important, create one instance per operation instead of trying to reuse
// it for long periods of time.
func New(
	svc *dynamodb.DynamoDB,
	tableName string,
	partitionKey string,
	partitionKeyType string,
	rangeKey string,
	rangeKeyType string,
) (*config, error) {
	data := &config{
		svc:                      svc,
		tableName:                tableName,
		partitionKey:             partitionKey,
		partitionKeyType:         partitionKeyType,
		rangeKey:                 rangeKey,
		rangeKeyType:             rangeKeyType,
		metaPrimaryKey:           getMetaPrimaryKey(partitionKey, partitionKeyType, rangeKey, rangeKeyType),
		snapshots:                make(map[string]*dynamodb.AttributeValue, 0),
		chronologicalSnapshotIDs: make([]string, 0),
	}

	// store local copies of the snapshot_name -> snapshot_id map and the chronologically sorted list of snapshot IDs
	err := data.cacheAllMetadata()
	if err != nil {
		return nil, errors.New("failed to cache metadata: " + err.Error())
	}

	return data, nil
}

func (s *config) Snapshot(snapshot string) (string, error) {
	_, ok := s.snapshots[snapshot]
	if ok {
		return "", errors.New("snapshot already exists: " + snapshot)
	}

	if s.currentSnapshotID != s.latestSnapshotID {
		return "", errors.New(fmt.Sprintf(
			"current snapshot (%s) does match latest (%s)",
			s.currentSnapshotID,
			s.latestSnapshotID,
		))
	}

	newID, err := s.getNextAvailableID()
	if err != nil {
		return "", errors.New("failed to get a snapshot ID:" + err.Error())
	}

	// update the snapshot_name --> snapshotID map with the new entry
	s.snapshots[snapshot] = &dynamodb.AttributeValue{
		S: aws.String(newID),
	}

	// update the ordered list of existing snapshots (IDs of the snapshots) new ID to the front because we always
	// start with the most recent snapshot
	s.chronologicalSnapshotIDs = append([]string{newID}, s.chronologicalSnapshotIDs...)
	// different data type for DynamoDB
	ids := []*dynamodb.AttributeValue{}
	for _, id := range s.chronologicalSnapshotIDs {
		ids = append(ids, &dynamodb.AttributeValue{S: aws.String(id)})
	}

	item := &dynamodb.UpdateItemInput{
		TableName: aws.String(s.tableName),
		Key:       s.metaPrimaryKey,
		ExpressionAttributeNames: map[string]*string{
			"#snapshots":  aws.String(ddbSnapshotsField),
			"#latestID":   aws.String(ddbLatestIDField),
			"#currentID":  aws.String(ddbCurrentIDField),
			"#orderedIDs": aws.String(ddbOrderedIDs),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":snapshots":  {M: s.snapshots},
			":latestID":   {S: aws.String(newID)},
			":orderedIDs": {L: ids},
		},
		UpdateExpression: aws.String(
			`SET #snapshots=:snapshots, #latestID=:latestID, #currentID=:latestID, #orderedIDs=:orderedIDs`,
		),
	}

	// use a conditional update to avoid race conditions update the metadata iff the the latest snapshotID has not
	// changed, i.e., there were no other snapshots were taken concurrently
	if s.latestSnapshotID != "" {
		item.ExpressionAttributeValues[":previousLatestID"] = &dynamodb.AttributeValue{
			S: aws.String(s.latestSnapshotID)}
		item.ConditionExpression = aws.String("#latestID=:previousLatestID")
	} else {
		item.ConditionExpression = aws.String("attribute_not_exists(#latestID)")
	}

	_, err = s.svc.UpdateItem(item)
	return newID, err
}

func (s *config) Rollback(snapshot string) (string, error) {
	var item *dynamodb.UpdateItemInput
	var id string
	var err error

	_, ok := s.snapshots[snapshot]
	if !ok && snapshot != "" {
		return "", errors.New(fmt.Sprintf("snapshot '%s' does not exist", snapshot))
	}

	// DynamoDB does not support empty strings so rolling back to "" == before any snapshots => remove the key
	if snapshot != "" {
		id, err = s.GetSnapshotID(snapshot)
		if err != nil {
			return "", err
		}

		item = &dynamodb.UpdateItemInput{
			TableName: aws.String(s.tableName),
			Key:       s.metaPrimaryKey,
			ExpressionAttributeNames:  map[string]*string{"#currentID": aws.String(ddbCurrentIDField)},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":currentID": {S: aws.String(id)}},
			UpdateExpression:          aws.String("SET #currentID=:currentID"),
		}
	} else {
		item = &dynamodb.UpdateItemInput{
			TableName: aws.String(s.tableName),
			Key:       s.metaPrimaryKey,
			ExpressionAttributeNames: map[string]*string{"#currentID": aws.String(ddbCurrentIDField)},
			UpdateExpression:         aws.String(fmt.Sprintf("REMOVE %s", ddbCurrentIDField)),
		}
	}

	// use a conditional update to avoid race conditions;
	// update the metadata iff the the current snapshotID has not changed, i.e., there were no other rollbacks
	// happening concurrently
	if s.currentSnapshotID != "" {
		if item.ExpressionAttributeValues == nil {
			item.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue, 0)
		}
		item.ExpressionAttributeValues[":previousCurrentID"] = &dynamodb.AttributeValue{
			S: aws.String(s.currentSnapshotID)}
		item.ConditionExpression = aws.String("#currentID=:previousCurrentID")
	} else {
		item.ConditionExpression = aws.String("attribute_not_exists(#currentID)")
	}

	_, err = s.svc.UpdateItem(item)

	return id, err
}

// ListSnapshots returns all existing snapshots
func (s *config) ListSnapshots() []string {
	return s.chronologicalSnapshotIDs
}

// return all snapshot IDs, chronologically sorted, starting with `first`
func (s *config) GetChronologicalSnapshotIDs(first string) []string {
	var ids []string = make([]string, 0)
	var i int

	// special cases
	switch first {
	case "":
		// just to avoid unnecessary loops as a minor performance improvement this really means "none" because there
		// are no snapshots before the first one was even taken
		return []string{""}
	case SNAPSHOT_LATEST:
		first = s.latestSnapshotID
	case SNAPSHOT_CURRENT:
		first = s.currentSnapshotID
	}

	// skip all IDs until `first`
	for i = 0; i < len(s.chronologicalSnapshotIDs); i++ {
		if s.chronologicalSnapshotIDs[i] == first {
			break
		}
	}

	// collect all remaining IDs
	for i < len(s.chronologicalSnapshotIDs) {
		ids = append(ids, s.chronologicalSnapshotIDs[i])
		i++
	}

	return ids
}

// GetSnapshotID returns the internal ID mapped to the given snapshot
func (s *config) GetSnapshotID(snapshot string) (string, error) {
	// special cases
	switch snapshot {
	case "":
		// empty string means no snapshot (before any were created), the corresponding ID is also an empty string
		return "", nil
	case SNAPSHOT_LATEST:
		return s.latestSnapshotID, nil
	case SNAPSHOT_CURRENT:
		return s.currentSnapshotID, nil
	}

	// at this point we just need to query the meta-data to try and find the snapshot
	id, ok := s.snapshots[snapshot]
	if ok {
		return *id.S, nil
	}

	return "", errors.New("snapshot '" + snapshot + "' does not exist")
}

// GetCurrentSnapshotID returns the ID of the snapshot currently set as active
// This can be the most recent one, or some past snapshot in the case of a rollback
func (s *config) GetCurrentSnapshotID() string {
	// snapshots exist because latest != "", so current == "" means we rolled back to "" (before any snapshots)
	if s.currentSnapshotID == "" && s.latestSnapshotID != "" {
		return ""
	}
	// there is an active snapshot (may, or may not, be a rollback)
	if s.currentSnapshotID != "" {
		return s.currentSnapshotID
	}
	//
	return s.latestSnapshotID
}

func (s *config) cacheAllMetadata() error {
	result, err := s.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key:       s.metaPrimaryKey,
	})
	if err != nil {
		return err
	}

	// snapshot_name -> snapshot
	snapshots, ok := result.Item[ddbSnapshotsField]
	if ok {
		s.snapshots = snapshots.M
	}

	// chronologically sorted snapshot IDs
	ids, ok := result.Item[ddbOrderedIDs]
	if ok {
		for i := 0; i < len(ids.L); i++ {
			s.chronologicalSnapshotIDs = append(s.chronologicalSnapshotIDs, *ids.L[i].S)
		}
	}

	// current snapshot
	current, ok := result.Item[ddbCurrentIDField]
	if ok {
		s.currentSnapshotID = *current.S
	}

	// latest snapshot
	latest, ok := result.Item[ddbLatestIDField]
	if ok {
		s.latestSnapshotID = *latest.S
	}

	return nil
}

// find and return the first available ID (integer not yet assigned to some snapshot)
func (s *config) getNextAvailableID() (string, error) {
	var i int64
	var free bool

	for i = 1; i < int64(math.Pow10(snapshotIDLength)); i++ {
		free = true
		for _, v := range s.snapshots {
			id, err := strconv.ParseInt(*v.S, 10, 64)
			if err != nil {
				return "", err
			}
			if id == i {
				free = false
				break
			}
		}
		if free {
			return strconv.Itoa(int(i)), nil
		}
	}

	return "", errors.New("no IDs left")
}

// return the primary key we need to use when querying the table for meta-data
// it may, or may not, include a range key; type of each key can be N or S
func getMetaPrimaryKey(
	partitionKey string,
	partitionKeyType string,
	rangeKey string,
	rangeKeyType string,
) map[string]*dynamodb.AttributeValue {
	key := make(map[string]*dynamodb.AttributeValue, 0)

	// we always have a partition key
	if partitionKeyType == "S" {
		key[partitionKey] = &dynamodb.AttributeValue{S: aws.String(ddbPartitionKey)}
	} else {
		key[partitionKey] = &dynamodb.AttributeValue{N: aws.String(ddbPartitionKey)}
	}

	// maybe there's a range key?
	if rangeKey != "" {
		if rangeKeyType == "S" {
			key[rangeKey] = &dynamodb.AttributeValue{S: aws.String(ddbRangeKey)}
		} else {
			key[rangeKey] = &dynamodb.AttributeValue{N: aws.String(ddbRangeKey)}
		}
	}

	return key
}
