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

package ddblibrarian_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/marcoalmeida/ddblibrarian"
)

const (
	tableName        = "Movies"
	partitionKey     = "year"
	rangeKey         = "title"
	partitionKeyType = "N"
	rangeKeyType     = "S"
	region           = "us-west-2"
	endpoint         = "http://localhost:8000"
)

var dataSets []string = []string{"example_batchjob_moviedata_1.json", "example_batchjob_moviedata_2.json"}

type movieInfo struct {
	Directors   []string `json:directors`
	ReleaseDate string   `json:release_date`
	Rating      float64  `json:rating`
	Genres      []string `json:genres`
	Image       string   `json:image_url`
	Plot        string   `json:plot`
	Rank        int64    `json:rank`
	RunningTime int64    `json:running_time_secs`
	Actors      []string `json:actors`
}

type movie struct {
	Year  int64     `json:year`
	Title string    `json:title`
	Info  movieInfo `json:info`
}

// create the table
func setup() error {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(region),
		Endpoint:   aws.String(endpoint),
		MaxRetries: aws.Int(1),
	})

	if err != nil {
		return err
	}

	ddbService := dynamodb.New(ddbSession)
	_, err = ddbService.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(partitionKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String(rangeKey),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(partitionKey),
				AttributeType: aws.String(partitionKeyType),
			},
			{
				AttributeName: aws.String(rangeKey),
				AttributeType: aws.String(rangeKeyType),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(100),
			WriteCapacityUnits: aws.Int64(100),
		},
	})

	return err
}

func teardown() error {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(region),
		Endpoint:   aws.String(endpoint),
		MaxRetries: aws.Int(1),
	})

	if err != nil {
		return err
	}

	ddbService := dynamodb.New(ddbSession)

	ddbService.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	return nil
}

func connect() (*ddblibrarian.Library, error) {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(region),
		Endpoint:   aws.String(endpoint),
		MaxRetries: aws.Int(1),
	})

	if err != nil {
		return nil, err
	}

	return ddblibrarian.New(tableName, partitionKey, partitionKeyType, rangeKey, rangeKeyType, ddbSession)
}

func readData(dataSource string) ([]movie, error) {
	jsonData, err := ioutil.ReadFile(dataSource)
	if err != nil {
		return nil, err
	}

	movies := make([]movie, 0)
	err = json.Unmarshal(jsonData, &movies)
	if err != nil {
		return nil, err
	}

	return movies, nil
}

func batchLoad(library *ddblibrarian.Library, movies []movie) {
	requests := make(map[string][]*dynamodb.WriteRequest, 0)

	for i, m := range movies {
		// write batches of 20 elements
		if i%20 == 0 && len(requests) > 0 {
			_, err := library.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems: requests,
			})
			if err != nil {
				fmt.Println("Failed to write batch:", err)
			}

			// start a new batch
			requests = make(map[string][]*dynamodb.WriteRequest, 0)
		}

		jsonData, err := json.Marshal(m.Info)
		if err != nil {
			fmt.Println("Failed to marshal info for", m.Title)
		}

		requests[tableName] = append(requests[tableName], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					partitionKey: {N: aws.String(strconv.Itoa(int(m.Year)))},
					rangeKey:     {S: aws.String(m.Title)},
					"info":       {B: jsonData},
				},
			}})

		if err != nil {
			fmt.Println("Failed to load", m.Title, ":", err.Error())
		}
	}

	// load whatever items are left
	if len(requests) > 0 {
		_, err := library.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: requests,
		})
		if err != nil {
			fmt.Println("Failed to write batch:", err)
		}
	}
}

func demoItem() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {N: aws.String("2012")},
			rangeKey:     {S: aws.String("The Avengers")},
		},
	}
}

func extractRating(data []byte) float64 {
	info := movieInfo{}
	err := json.Unmarshal(data, &info)
	if err != nil {
		return -1
	}

	return info.Rating
}

func showItem(library *ddblibrarian.Library) {
	out, err := library.GetItem(demoItem())

	if err != nil {
		fmt.Println("Failed to GetItem:", err.Error())
	} else {
		fmt.Printf("Rating: %.1f\n", extractRating(out.Item["info"].B))
	}
}

func showItemFromSnapshot(library *ddblibrarian.Library, snapshot string) {
	out, err := library.GetItemFromSnapshot(demoItem(), snapshot)

	if err != nil {
		fmt.Println("Failed to GetItemFromSnapshot:", err.Error())
	} else {
		fmt.Printf("Rating: %.1f\n", extractRating(out.Item["info"].B))
	}
}

func demo(library *ddblibrarian.Library) {
	fmt.Println("=====================")
	// show the initial data for 'The Avengers'
	fmt.Println("Active snapshot:")
	showItem(library)
	fmt.Println()

	// use GetItemFromSnapshot to show that we still have the data saved from `initial-load`
	fmt.Println("From the snapshot 'batch-0':")
	showItemFromSnapshot(library, "batch-0")
	fmt.Println()

	// Rollback so that all operations, from all clients, will use data from `initial-load`
	fmt.Println("Rolling back to snapshot 'batch-0'...")
	library.Rollback("batch-0")
	fmt.Println()

	// plain GetItem retrieves the good data
	fmt.Println("Active snapshot:")
	showItem(library)
}

// This example demonstrates how to use dynamodb-librarian to keep multiple versions of a given item when updating
// a table with some recurrent batch job that overwrites a significant number of items created by the previous run.
//
// In this example, the second run of the batch job loads one item with corrupt data. We use the field 'Rating' to
// show the differences.
//
// In order to recover the previous, healthy, version of the data, we can either explicitly retrieve it from an earlier
// snapshot or do a rollback and set that as the default version to all subsequent queries.
func Example_batchJob() {
	// create the table
	err := setup()
	if err != nil {
		fmt.Println(err.Error())
	}

	// connect to the table using dynamodb-librarian
	library, err := connect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// run N batch jobs
	for i, ds := range dataSets {
		// take a snapshot before
		fmt.Println(fmt.Sprintf("Taking a snapshot before batch job %d...", i))
		err := library.Snapshot(fmt.Sprintf("batch-%d", i))
		if err != nil {
			fmt.Println(err.Error())
		}
		// read the data
		fmt.Println(fmt.Sprintf("Readind movie dataset %d...", i))
		movies, err := readData(ds)
		if err != nil {
			fmt.Println(err.Error())
		}
		// load our sample data to the table
		fmt.Println(fmt.Sprintf("Running batch job %d...", i))
		batchLoad(library, movies)
	}

	// show what we can do with dynamodb-librarian
	demo(library)

	// cleanup
	err = teardown()
	if err != nil {
		fmt.Println(err.Error())
	}

	// Output:
	//Taking a snapshot before batch job 0...
	//Readind movie dataset 0...
	//Running batch job 0...
	//Taking a snapshot before batch job 1...
	//Readind movie dataset 1...
	//Running batch job 1...
	//=====================
	//Active snapshot:
	//Rating: -5.0
	//
	//From the snapshot 'batch-0':
	//Rating: 8.2
	//
	//Rolling back to snapshot 'batch-0'...
	//
	//Active snapshot:
	//Rating: 8.2
}
