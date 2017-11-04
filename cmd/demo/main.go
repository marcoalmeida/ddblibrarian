package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/marcoalmeida/dynamodb-librarian"
)

const (
	tableName        = "Movies"
	partitionKey     = "year"
	rangeKey         = "title"
	partitionKeyType = "N"
	rangeKeyType     = "S"
	region           = "us-west-2"
	endpoint         = "http://localhost:8000"
	dataSource       = "moviedata.json"
)

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

func readData() ([]movie, error) {
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

func connect() (*librarian.Library, error) {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(region),
		Endpoint:   aws.String(endpoint),
		MaxRetries: aws.Int(1),
	})

	if err != nil {
		return nil, err
	}

	return librarian.New(tableName, partitionKey, partitionKeyType, rangeKey, rangeKeyType, ddbSession)
}

func loadData(keeper *librarian.Library, movies []movie) {
	fmt.Println("Loading movie data...\n")
	for _, m := range movies {
		jsonData, err := json.Marshal(m.Info)
		if err != nil {
			fmt.Println("Failed to marshal info for", m.Title)
		}

		_, err = keeper.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]*dynamodb.AttributeValue{
				partitionKey: {N: aws.String(strconv.Itoa(int(m.Year)))},
				rangeKey:     {S: aws.String(m.Title)},
				"info":       {B: jsonData},
			},
		})

		if err != nil {
			fmt.Println("Failed to load", m.Title, ":", err.Error())
		}
	}
}

func showItem(record *librarian.Library) {
	out, err := record.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {N: aws.String("2012")},
			rangeKey:     {S: aws.String("The Avengers")},
		},
	})

	if err != nil {
		fmt.Println("Failed to GetItem:", err.Error())
	} else {
		fmt.Printf("\t%s, %s: %s\n", *out.Item["title"].S, *out.Item["year"].N, string(out.Item["info"].B))
	}
}

func showItemFromSnapshot(record *librarian.Library, snapshot string) {
	out, err := record.GetItemFromSnapshot(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {N: aws.String("2012")},
			rangeKey:     {S: aws.String("The Avengers")},
		},
	},
		snapshot,
	)

	if err != nil {
		fmt.Println("Failed to GetItem:", err.Error())
	} else {
		fmt.Printf("\t%s, %s: %s\n", *out.Item["title"].S, *out.Item["year"].N, string(out.Item["info"].B))
	}
}

func demo(record *librarian.Library) {
	// show the initial data for 'The Avengers'
	fmt.Println("=> Movie details -- active entry")
	showItem(record)
	fmt.Println()

	// take a snapshot before making any changes
	fmt.Println("=> Taking a snapshot, before manual updates...")
	record.Snapshot("backup1")
	fmt.Println()

	// accidentally destroys The Avengers
	fmt.Println("=> Updating movie data (i.e., introducing errors)...")
	record.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]*dynamodb.AttributeValue{
			partitionKey: {N: aws.String("2012")},
			rangeKey:     {S: aws.String("The Avengers")},
			"info":       {B: []byte("{\"This used to contain important movie information\"}")},
		},
	})
	fmt.Println()

	// show the mess
	fmt.Println("=> Movie details -- active entry")
	showItem(record)
	fmt.Println()

	// use GetItemFromSnapshot to show that we still have the data saved from `initial-load`
	fmt.Println("=> Movie details -- from the snapshot")
	showItemFromSnapshot(record, "")
	fmt.Println()

	// Rollback so that all operations, from all clients, will use data from `initial-load`
	fmt.Println("=> Rolling back to the initial snapshot...")
	record.Rollback("")
	fmt.Println()

	// plain GetItem retrieves the good data
	fmt.Println("=> Movie details -- active entry")
	showItem(record)
}

func main() {
	// create the table
	err := setup()
	if err != nil {
		fmt.Println(err.Error())
	}

	// read the sample data
	movies, err := readData()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// connect to the table using dynamodb-librarian
	record, err := connect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// load our sample data to the table
	loadData(record, movies)

	// run the demo
	demo(record)

	// cleanup
	err = teardown()
	if err != nil {
		fmt.Println(err.Error())
	}
}
