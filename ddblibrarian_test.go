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

package ddblibrarian

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	ddbTableName = "dynamodb-librarian"
	ddbRegion    = "local"
	ddbEndpoint  = "http://localhost:8000"
)

var ddbService *dynamodb.DynamoDB
var ddbSession *session.Session
var pkType string

// we always need to test things on 4 different schemas: (simple or composite indexes) x (string or number)
// the following set of constants allows us to index the common parameters by the schema being tested
const (
	SIMPLE_S = iota
	COMPOSITE_S
	SIMPLE_N
	COMPOSITE_N
)

var possibleSchemas = []int{SIMPLE_S, COMPOSITE_S, SIMPLE_N, COMPOSITE_N}

// there's no point on having the partition key indexed by schema as it's always present
var partitionKey = "partition_key"

var partitionKeyType = map[int]string{
	SIMPLE_S:    "S",
	COMPOSITE_S: "S",
	SIMPLE_N:    "N",
	COMPOSITE_N: "N",
}

var rangeKey = map[int]string{
	SIMPLE_S:    "",
	COMPOSITE_S: "range_key",
	SIMPLE_N:    "",
	COMPOSITE_N: "range_key",
}

var rangeKeyType = map[int]string{
	SIMPLE_S:    "",
	COMPOSITE_S: "S",
	SIMPLE_N:    "",
	COMPOSITE_N: "N",
}

var valueField = "value"

var readCapacity = 100
var writeCapacity = 100

var keySchema = map[int][]*dynamodb.KeySchemaElement{
	SIMPLE_S: {
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(partitionKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	},
	COMPOSITE_S: {
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(partitionKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(rangeKey[COMPOSITE_S]),
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		},
	},
	SIMPLE_N: {
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(partitionKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	},
	COMPOSITE_N: {
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(partitionKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(rangeKey[COMPOSITE_N]),
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		},
	},
}

var attributeDefinitions = map[int][]*dynamodb.AttributeDefinition{
	SIMPLE_S: {
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: aws.String("S"),
		},
	},
	COMPOSITE_S: {
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: aws.String("S"),
		},
		{
			AttributeName: aws.String(rangeKey[COMPOSITE_S]),
			AttributeType: aws.String("S"),
		},
	},
	SIMPLE_N: {
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: aws.String("N"),
		},
	},
	COMPOSITE_N: {
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: aws.String("N"),
		},
		{
			AttributeName: aws.String(rangeKey[COMPOSITE_N]),
			AttributeType: aws.String("N"),
		},
	},
}

var provisionedThroughput = map[int]*dynamodb.ProvisionedThroughput{
	SIMPLE_S: {
		ReadCapacityUnits:  aws.Int64(int64(readCapacity)),
		WriteCapacityUnits: aws.Int64(int64(writeCapacity)),
	},
	COMPOSITE_S: {
		ReadCapacityUnits:  aws.Int64(int64(readCapacity)),
		WriteCapacityUnits: aws.Int64(int64(writeCapacity)),
	},
	SIMPLE_N: {
		ReadCapacityUnits:  aws.Int64(int64(readCapacity)),
		WriteCapacityUnits: aws.Int64(int64(writeCapacity)),
	},
	COMPOSITE_N: {
		ReadCapacityUnits:  aws.Int64(int64(readCapacity)),
		WriteCapacityUnits: aws.Int64(int64(writeCapacity)),
	},
}

func getAttributeValueForKey(schema int) map[string]*dynamodb.AttributeValue {
	// a number works for both data types
	pk := "1234"
	rk := "5678"

	switch schema {
	default:
		fallthrough
	case SIMPLE_S:
		return map[string]*dynamodb.AttributeValue{
			partitionKey: {S: aws.String(pk)}}
	case SIMPLE_N:
		return map[string]*dynamodb.AttributeValue{
			partitionKey: {N: aws.String(pk)}}
	case COMPOSITE_S:
		return map[string]*dynamodb.AttributeValue{
			partitionKey:     {S: aws.String(pk)},
			rangeKey[schema]: {S: aws.String(rk)},
		}
	case COMPOSITE_N:
		return map[string]*dynamodb.AttributeValue{
			partitionKey:     {N: aws.String(pk)},
			rangeKey[schema]: {N: aws.String(rk)},
		}
	}
}

func getAttributeValueForItem(schema int, valueTag string) map[string]*dynamodb.AttributeValue {
	// use the base construction for a Key, add the value field
	base := getAttributeValueForKey(schema)
	base[valueField] = &dynamodb.AttributeValue{S: aws.String(fmtValueTag(valueTag))}

	return base
}

func fmtValueTag(valueTag string) string {
	value := "some data"

	if valueTag != "" {
		value += fmt.Sprintf("(after *%s*)", valueTag)
	}

	return value
}

// create a different table name for each schema -- create/delete operations
// take a while and running tests sequentially may result on "key element does not match the schema"
// errors
func getTableName(schema int) string {
	return fmt.Sprintf("%s-%d", ddbTableName, schema)
}

func setupTest(schema int, t *testing.T) (*Library, func(test int, t *testing.T)) {
	t.Log("setting up schema", schema, "on table", getTableName(schema))
	var err error

	ddbSession, err = session.NewSession(&aws.Config{
		Region:     aws.String(ddbRegion),
		Endpoint:   aws.String(ddbEndpoint),
		MaxRetries: aws.Int(1),
	})
	if err != nil {
		t.Error(err)
	}

	ddbService = dynamodb.New(ddbSession)
	_, err = ddbService.CreateTable(&dynamodb.CreateTableInput{
		TableName:             aws.String(getTableName(schema)),
		KeySchema:             keySchema[schema],
		AttributeDefinitions:  attributeDefinitions[schema],
		ProvisionedThroughput: provisionedThroughput[schema],
	})
	if err != nil {
		t.Log("Table already exists. Skipping.")
	}

	status := ""
	for status != "ACTIVE" {
		t.Log("Waiting for table to be created...")
		time.Sleep(1000 * time.Millisecond)
		response, err := ddbService.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(getTableName(schema))})
		if err != nil {
			// ignore -- may be caused by issues related to eventual consistency
		}
		status = *response.Table.TableStatus
	}

	// fail on purpose: librarian.New()
	_, err = New(
		getTableName(schema),
		partitionKey,
		"nope",
		rangeKey[schema],
		rangeKeyType[schema],
		ddbSession,
	)
	if err == nil {
		t.Error("Expected to fail")
	}

	client, err := New(
		getTableName(schema),
		partitionKey,
		partitionKeyType[schema],
		rangeKey[schema],
		rangeKeyType[schema],
		ddbSession,
	)
	if err != nil {
		t.Error(err.Error())
	}

	return client, func(schema int, t *testing.T) {
		t.Log("tearing down schema", schema)
		ddbService.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(getTableName(schema)),
		})

		status := "DELETING"
		for status == "DELETING" {
			t.Log("Waiting for table to be deleted...")
			time.Sleep(1000 * time.Millisecond)
			res, err := ddbService.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(getTableName(schema))})
			if err != nil {
				// table has been deleted
				return
			}
			status = *res.Table.TableStatus
		}
	}
}

// make sure our PutItem/GetItem methods do not modify the item
func TestNoModifications(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// write and assert the PK did not change
		putInput := &dynamodb.PutItemInput{
			TableName: aws.String(getTableName(schema)),
			Item:      getAttributeValueForItem(schema, ""),
		}
		value := *putInput.Item[partitionKey]
		library.PutItem(putInput)
		if !reflect.DeepEqual(*putInput.Item[partitionKey], value) {
			t.Error(
				"Expected PK value", value,
				"got", *putInput.Item[partitionKey],
				"after PutItem",
			)
		}

		// read and assert the PK did not change
		getInput := &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		value = *putInput.Item[partitionKey]
		library.GetItem(getInput)
		if !reflect.DeepEqual(*getInput.Key[partitionKey], value) {
			t.Error(
				"Expected PK value", value,
				"got", *putInput.Item[partitionKey],
				"after GetItem",
			)
		}

		teardown(schema, t)
	}
}

// test writing and reading data
// providing no snapshot info (i.e., retrieve the latest), a non-existent snapshot,
// and no snapshot (i.e., before there were any)
func TestNoSnapshots(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// write an item
		putInput := &dynamodb.PutItemInput{
			TableName: aws.String(getTableName(schema)),
			Item:      getAttributeValueForItem(schema, ""),
		}
		_, err := library.PutItem(putInput)
		if err != nil {
			t.Error(err)
		}
		// read the same key
		getInput := &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		// simple read -- expect the same value that was provided for PutItem
		data, err := library.GetItem(getInput)
		if err != nil {
			t.Error(
				"Expected no errors",
				"item:", getInput,
				"got", err,
			)
		} else {
			if !reflect.DeepEqual(putInput.Item, data.Item) {
				t.Error(
					"Data mismatch:",
					"expected", putInput.Item,
					"got", data.Item,
				)
			}
		}
		// empty snapshot: expect the same value
		data, err = library.GetItemFromSnapshot(getInput, "")
		if err != nil {
			t.Error(
				"Expected no errors",
				"item:", getInput, "snapshot: ''",
				"got", err,
			)
		} else {
			if !reflect.DeepEqual(putInput.Item, data.Item) {
				t.Error(
					"Expected", putInput.Item,
					"got", data.Item, ", empty snapshot",
				)
			}
		}
		// non-existent snapshot: expect an error
		_, err = library.GetItemFromSnapshot(getInput, "nope")
		if err == nil {
			t.Error(
				"Expected an error",
				"item:", getInput, "snapshot: 'nope'",
				"got", err,
			)
		}

		teardown(schema, t)
	}
}

// make sure no errors are throw and that the current snapshot ID is updated locally but *and* on the meta-data
func TestRollback(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// expect an error when rolling back to some snapshot that does not exit
		err := library.Rollback("nope")
		if err == nil {
			t.Error("Expected an error")
		}

		// take a couple of snapshots
		for _, s := range []string{"snap1", "snap2"} {
			err := library.Snapshot(s)
			if err != nil {
				t.Error(err)
			}
		}

		// expect no errors when rolling back
		for _, s := range []string{"snap1", ""} {
			err := library.Rollback(s)
			if err != nil {
				t.Error(err.Error())
			}
		}

		teardown(schema, t)
	}
}

// make sure no errors are throw and that the current snapshot ID is updated *locally* but not on the meta-data
func TestBrowse(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// there should be no snapshots yet
		if library.currentSnapshot != "" {
			t.Error(
				"Expected empty ID querying the current snapshot",
				"got ID", library.currentSnapshot,
			)
		}

		// take a couple of snapshots
		for _, s := range []string{"snap1", "snap2"} {
			err := library.Snapshot(s)
			if err != nil {
				t.Error(err)
			}
		}
		library.Browse("snap1")
		// expect some ID
		if library.currentSnapshot == "" {
			t.Error(
				"Expected some ID querying the current snapshot",
				"got and empty string",
			)
		}

		teardown(schema, t)
	}
}

func TestSnapshot(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		err := library.Snapshot("")
		if err == nil {
			t.Error("expected error when creating empty snapshot")
		}

		err = library.Snapshot("hello")
		if err != nil {
			t.Error(err)
		}

		err = library.Snapshot("hello")
		if err == nil {
			t.Error("expected error on duplicate snapshot name")
		}

		teardown(schema, t)
	}
}

func TestLibrary_ListSnapshots(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		existingIDs, err := library.ListSnapshots()
		if err != nil {
			t.Error(err.Error())
		}

		if len(existingIDs) != 0 {
			t.Error("Expected no snapshot IDs, got", existingIDs)
		}

		var snapshots = []string{"first", "second", "third"}
		// var expectedIDs = []string{}
		for _, s := range snapshots {
			err := library.Snapshot(s)
			if err != nil {
				t.Error(err)
			}
		}

		existingIDs, err = library.ListSnapshots()
		if err != nil {
			t.Error(err.Error())
		}

		if len(existingIDs) != len(snapshots) {
			t.Error("Expected", len(snapshots), "snapshot IDs, got", existingIDs)
		}

		teardown(schema, t)
	}
}

func TestLibrary_GetItem(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// table does not exist, expect to fail
		input := &dynamodb.GetItemInput{
			TableName: aws.String("nope"),
			Key:       getAttributeValueForKey(schema),
		}
		_, err := library.GetItem(input)
		if err == nil {
			t.Error("expected error as table does not exist")
		}

		// should not fail but does not exist
		input = &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err := library.GetItem(input)
		if err != nil {
			t.Error("expected no errors, got", err)
		}
		// expect an empty return value as the item did not exist
		if out.Item != nil {
			t.Error("expected empty result, got", out)
		}

		// save a few items, always take a snapshot
		values := make(map[int]string, 0)
		for i := 0; i < 3; i++ {
			inputPut := &dynamodb.PutItemInput{
				TableName: aws.String(getTableName(schema)),
				Item:      getAttributeValueForItem(schema, fmt.Sprintf("data_%d", i)),
			}
			values[i] = *inputPut.Item[valueField].S
			library.PutItem(inputPut)
			library.Snapshot(strconv.Itoa(i))
		}
		// Get the most recent element
		input = &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err = library.GetItem(input)
		if err != nil {
			t.Error("expected no errors, got:", err)
		}
		if *out.Item[valueField].S != values[2] {
			t.Error("expected most recent item:", values[2], ", got:", *out.Item[valueField].S)
		}
		// Get the initial value, before any snapshots
		input = &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err = library.GetItemFromSnapshot(input, "")
		if err != nil {
			t.Error("expected no errors, got:", err)
		}

		// expect an empty return value as the item did not exist
		if *out.Item[valueField].S != values[0] {
			t.Error("expected most recent item,", values[0], "got", *out.Item[valueField].S)
		}

		teardown(schema, t)
	}
}

func TestLibrary_UpdateItem(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// table does not exist, expect to fail
		input := &dynamodb.UpdateItemInput{
			TableName: aws.String("nope"),
			Key:       getAttributeValueForKey(schema),
		}
		_, err := library.UpdateItem(input)
		if err == nil {
			t.Error("Expected error as table does not exist")
		}

		// does not exist, should insert
		input = &dynamodb.UpdateItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		_, err = library.UpdateItem(input)
		if err != nil {
			t.Error(err.Error())
		}

		read, err := library.GetItem(
			&dynamodb.GetItemInput{
				TableName: aws.String(getTableName(schema)),
				Key:       getAttributeValueForKey(schema),
			})
		if err != nil {
			t.Error(err.Error())
		}

		if !reflect.DeepEqual(read.Item[partitionKey], input.Key[partitionKey]) {
			t.Error("Expected", input.Key[partitionKey], "got", read.Item[partitionKey])
		}

		teardown(schema, t)
	}
}

func TestLibrary_DeleteItem(t *testing.T) {
	for _, schema := range possibleSchemas {
		library, teardown := setupTest(schema, t)

		// table does not exist, expect to fail
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String("nope"),
			Key:       getAttributeValueForKey(schema),
		}
		_, err := library.DeleteItem(input)
		if err == nil {
			t.Error("expected error as table does not exist")
		}

		// should not fail but does not exist
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err := library.DeleteItem(input)
		if err != nil {
			t.Error("expected no errors, got", err)
		}
		// expect an empty return value as the item did not exist
		if out.Attributes != nil {
			t.Error("expected empty result, got", out)
		}

		// save a few items, always take a snapshot
		values := make(map[int]string, 0)
		for i := 0; i < 3; i++ {
			inputPut := &dynamodb.PutItemInput{
				TableName: aws.String(getTableName(schema)),
				Item:      getAttributeValueForItem(schema, fmt.Sprintf("data_%d", i)),
			}
			values[i] = *inputPut.Item[valueField].S
			library.PutItem(inputPut)
			library.Snapshot(strconv.Itoa(i))
		}
		// delete the most recent element
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err = library.DeleteItem(input)
		if err != nil {
			t.Error("expected no errors, got:", err)
		}
		if *out.Attributes[valueField].S != values[2] {
			t.Error("expected most recent item:", values[2], ", got:", *out.Attributes[valueField].S)
		}
		// delete the initial value, before any snapshots
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		out, err = library.DeleteItemFromSnapshot(input, "")
		if err != nil {
			t.Error("expected no errors, got:", err)
		}

		// expect an empty return value as the item did not exist
		if *out.Attributes[valueField].S != values[0] {
			t.Error("expected most recent item,", values[0], "got", *out.Attributes[valueField].S)
		}

		teardown(schema, t)
	}
}

func TestLibrary_GeneralUsage(t *testing.T) {
	for _, schema := range possibleSchemas {
		library := make([]*Library, 2)
		var teardown func(schema int, t *testing.T)

		library[0], teardown = setupTest(schema, t)
		library[1], _ = setupTest(schema, t)

		// throughout this test we'll need to know what value was used on a given snapshot
		snapshotValue := map[string]string{
			"":        "pre-librarian",
			"backup1": "post-backup1",
			"backup2": "post-backup2",
		}
		// we need to create snapshots and write data in the right order -- the map is no bueno
		snapshotsOrder := []string{"", "backup1", "backup2"}
		// put all items and take the snapshots
		for _, s := range snapshotsOrder {
			// take a snapshot (unless writing the pre-snapshot data)
			if s != "" {
				err := library[0].Snapshot(s)
				if err != nil {
					t.Error(err)
				}
			}

			putInput := &dynamodb.PutItemInput{
				TableName: aws.String(getTableName(schema)),
				Item:      getAttributeValueForItem(schema, snapshotValue[s]),
			}
			_, err := library[0].PutItem(putInput)
			if err != nil {
				t.Error(err)
			}
		}

		// make sure both clients read default to reading the latest update
		getInput := &dynamodb.GetItemInput{
			TableName: aws.String(getTableName(schema)),
			Key:       getAttributeValueForKey(schema),
		}
		output := make([]*dynamodb.GetItemOutput, 2)
		outputErr := make([]error, 2)
		for i := 0; i < 2; i++ {
			output[i], outputErr[i] = library[i].GetItem(getInput)
			if outputErr[i] != nil {
				t.Error(
					"Expected no errors",
					"item:", output[i],
					"got", outputErr[i],
				)
			}
		}
		if !reflect.DeepEqual(output[0].Item, output[1].Item) {
			t.Error(
				"Data mismatch between clients:",
				"items:\n", output[0].Item,
				"\nand\n", output[1].Item,
			)
		}

		if *output[0].Item[valueField].S != fmtValueTag(snapshotValue["backup2"]) {
			t.Error(
				"Data mismatch on schema", schema,
				"expected\n", fmtValueTag(snapshotValue["backup2"]),
				"\ngot\n", *output[0].Item[valueField].S,
			)
		}

		// enable browsing on one of the clients and make sure it defaults to
		// whatever value was stored on that snapshot;
		// confirm the other one still defaults to the last version
		library[1].Browse("backup1")
		for i := 0; i < 2; i++ {
			output[i], outputErr[i] = library[i].GetItem(getInput)
			if outputErr[i] != nil {
				t.Error(
					"Expected no errors",
					"item:", output[i],
					"got", outputErr[i],
				)
			}
		}
		if *output[0].Item[valueField].S != fmtValueTag(snapshotValue["backup2"]) {
			t.Error("backup2: expected:", fmtValueTag(snapshotValue["backup2"]), "got:", *output[0].Item[valueField].S)
		}
		if *output[1].Item[valueField].S != fmtValueTag(snapshotValue["backup1"]) {
			t.Error("backup1: expected:", fmtValueTag(snapshotValue["backup1"]), "got:", *output[1].Item[valueField].S)
		}

		// rollback using the first client and make sure the other one is still browsing the same snapshot
		library[0].Rollback("")
		for i := 0; i < 2; i++ {
			output[i], outputErr[i] = library[i].GetItem(getInput)
			if outputErr[i] != nil {
				t.Error(
					"Expected no errors",
					"item:", output[i],
					"got", outputErr[i],
				)
			}
		}
		if *output[0].Item[valueField].S != fmtValueTag(snapshotValue[""]) {
			t.Error("pre-snapshot: expected:", fmtValueTag(snapshotValue[""]), "got:", *output[0].Item[valueField].S)
		}
		if *output[1].Item[valueField].S != fmtValueTag(snapshotValue["backup1"]) {
			t.Error("backup1: expected:", fmtValueTag(snapshotValue["backup1"]), "got:", *output[1].Item[valueField].S)
		}

		// rollback to a previous snapshot and make sure both clients get the same data
		library[1].Rollback("backup1")
		for i := 0; i < 2; i++ {
			output[i], outputErr[i] = library[i].GetItem(getInput)
			if outputErr[i] != nil {
				t.Error(
					"Expected no errors",
					"item:", output[i],
					"got", outputErr[i],
				)
			}
			if *output[i].Item[valueField].S != fmtValueTag(snapshotValue["backup1"]) {
				t.Error(i, "backup1: expected:", fmtValueTag(snapshotValue["backup1"]), "got:", *output[i].Item[valueField].S)
			}
		}

		teardown(schema, t)
	}
}

// Examples

// Create a new session for the DynamoDB service, create a new instance of ddblibrarian (for some existing table,
// passing along the session), and list all existing snapshots as an example of basic usage.
//
// Note: error handling has been greatly simplified. Please make sure not to just copy-past this to a live,
// production system.
func ExampleNew() {
	// Create a new session for the DynamoDB client
	s, err := session.NewSession(&aws.Config{
		Region:     aws.String("us-east-1"),
		Endpoint:   aws.String("http://localhost:8000"),
		MaxRetries: aws.Int(3),
	})
	if err != nil {
		log.Fatalln(err)
	}
	// Create a new ddblibrarian instance
	library, err := New("example", "year", "N", "", "", s)
	if err != nil {
		log.Fatalln(err)
	}
	// List all existing snapshots
	snapshots, err := library.ListSnapshots()
	if err != nil {
		log.Fatalln(err)
	}

	for _, s := range snapshots {
		fmt.Println(s)
	}

	// Output:
}
