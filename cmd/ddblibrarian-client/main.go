package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/marcoalmeida/ddblibrarian"
)

type appConfig struct {
	region           string
	endpoint         string
	table            string
	partitionKey     string
	partitionKeyType string
	rangeKey         string
	rangeKeyType     string
	list             bool
	snapshot         string
	rollback         string
}

// make sure all required flags were passed and are valid
func checkFlags(app *appConfig) {
	if app.table == "" {
		log.Fatal("Please tell me which DynamoDB table to use")
	}

	if app.partitionKey == "" {
		log.Fatal("The partition key is required")
	}

	if app.partitionKeyType == "" {
		log.Fatal("The partition key type (S or N) is required")
	}

	if app.snapshot != "" && app.rollback != "" {
		log.Fatal("These are mutually exclusive options: snapshot, rollback")
	}
}

func connect(app *appConfig) *ddblibrarian.Library {
	ddbSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(app.region),
		Endpoint:   aws.String(app.endpoint),
		MaxRetries: aws.Int(1),
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	client, err := ddblibrarian.New(
		app.table,
		app.partitionKey,
		app.partitionKeyType,
		app.rangeKey,
		app.rangeKeyType,
		ddbSession,
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	return client
}

func executeActions(library *ddblibrarian.Library, app *appConfig) {
	if app.rollback != "" {
		err := library.Rollback(app.rollback)
		if err != nil {
			log.Fatal("Failed to rollback to snapshot", app.rollback, ":", err.Error())
		}
	}

	if app.snapshot != "" {
		err := library.Snapshot(app.snapshot)
		if err != nil {
			log.Fatal("Failed to create snapshot:", err.Error())
		}
	}

	// this can be combined with other options; leaving it in the end
	// allows us to easily show the state of the world
	if app.list {
		snapshots, err := library.ListSnapshots()
		if err != nil {
			log.Fatal("Failed to enumerate snapshots:", err.Error())
		}

		fmt.Println("Existing snapshots:")
		for _, s := range snapshots {
			fmt.Println(s)
		}
	}
}

func main() {
	app := &appConfig{}

	flag.StringVar(&app.region, "region", "us-east-1", "AWS region the table lives in")
	flag.StringVar(&app.endpoint, "endpoint", "", "Entry point for the DynamoDB region")
	flag.StringVar(&app.table, "table", "", "Name of the DynamoDB table")
	flag.StringVar(&app.partitionKey, "partition-key", "", "Partition key")
	flag.StringVar(&app.partitionKeyType, "partition-key-type", "", "Type of partition key (S or N)")
	flag.StringVar(&app.rangeKey, "range-key", "", "range key")
	flag.StringVar(&app.rangeKeyType, "range-key-type", "", "Type of range key (S or N)")
	flag.StringVar(&app.snapshot, "snapshot", "", "Take a snapshot")
	flag.StringVar(&app.rollback, "rollback", "", "Rollback to an existing snapshot")
	flag.BoolVar(&app.list, "list", false, "Lit existing snapshots")

	flag.Parse()

	checkFlags(app)

	library := connect(app)
	executeActions(library, app)
}
