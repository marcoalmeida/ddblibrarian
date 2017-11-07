package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/marcoalmeida/ddblibrarian"
)

type ClientType int

const (
	librarian ClientType = iota
	plainSdk
)

type Client struct {
	librarian *ddblibrarian.Library
	plainsdk  *dynamodb.DynamoDB
}

var tableName map[ClientType]string = map[ClientType]string{
	librarian: "Movies-Librarian",
	plainSdk:  "Movies-PlainSDK",
}

const (
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
func setup(c ClientType) error {
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
		TableName: aws.String(tableName[c]),
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

func teardown(c ClientType) error {
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
		TableName: aws.String(tableName[c]),
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

func connect(c ClientType) (*Client, error) {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(region),
		Endpoint:   aws.String(endpoint),
		MaxRetries: aws.Int(1),
	})

	if err != nil {
		return nil, err
	}

	librarian, err := ddblibrarian.New(tableName[c], partitionKey, partitionKeyType, rangeKey, rangeKeyType, ddbSession)
	if err != nil {
		return nil, err
	}

	return &Client{
		librarian: librarian,
		plainsdk:  dynamodb.New(ddbSession),
	}, nil
}

func loadData(client *Client, clientType ClientType, movies []movie) {
	fmt.Println("Loading movie data...\n")
	for _, m := range movies {
		jsonData, err := json.Marshal(m.Info)
		if err != nil {
			fmt.Println("Failed to marshal info for", m.Title)
		}

		input := &dynamodb.PutItemInput{
			TableName: aws.String(tableName[clientType]),
			Item: map[string]*dynamodb.AttributeValue{
				partitionKey: {N: aws.String(strconv.Itoa(int(m.Year)))},
				rangeKey:     {S: aws.String(m.Title)},
				"info":       {B: jsonData},
			},
		}

		if clientType == librarian {
			_, err = client.librarian.PutItem(input)
		} else {
			_, err = client.plainsdk.PutItem(input)
		}

		if err != nil {
			fmt.Println("Failed to load", m.Title, ":", err.Error())
		}
	}
}

func BenchmarkLibrarian(b *testing.B) {
	// create the table
	err := setup(librarian)
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
	client, err := connect(librarian)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	b.ResetTimer()
	// load our sample data to the table
	loadData(client, librarian, movies)

	teardown(librarian)
}

func BenchmarkPlainSDK(b *testing.B) {
	// create the table
	err := setup(plainSdk)
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
	client, err := connect(plainSdk)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	b.ResetTimer()
	// load our sample data to the table
	loadData(client, plainSdk, movies)

	teardown(plainSdk)
}
