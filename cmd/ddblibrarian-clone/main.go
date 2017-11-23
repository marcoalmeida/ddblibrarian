package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/marcoalmeida/ddblibrarian"
)

type appConfig struct {
	srcRegion        string
	dstRegion        string
	srcTable         string
	dstTable         string
	partitionKey     string
	partitionKeyType string
	rangeKey         string
	rangeKeyType     string
	snapshot         string
}

func checkFlags(app *appConfig) {
	if app.srcTable == "" || app.dstTable == "" {
		log.Fatal("Both source and destination tables are mandatory")
	}

	if app.partitionKey == "" {
		log.Fatal("The partition key is required")
	}

	if app.partitionKeyType == "" {
		log.Fatal("The partition key type (S or N) is required")
	}
}

func connect(app *appConfig) (*dynamodb.DynamoDB, *ddblibrarian.Library) {
	srcSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(app.srcRegion),
		MaxRetries: aws.Int(3),
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	dstSession, err := session.NewSession(&aws.Config{
		Region:     aws.String(app.dstRegion),
		MaxRetries: aws.Int(3),
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	librarian, err := ddblibrarian.New(
		app.dstTable,
		app.partitionKey,
		app.partitionKeyType,
		app.rangeKey,
		app.rangeKeyType,
		dstSession,
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	return dynamodb.New(srcSession), librarian
}

func writeItems(
	items []map[string]*dynamodb.AttributeValue,
	lastEvaluatedKey map[string]*dynamodb.AttributeValue,
	library *ddblibrarian.Library, app *appConfig,
) {
	requests := make(map[string][]*dynamodb.WriteRequest, 0)

	for _, item := range items {
		requests[app.dstTable] = append(requests[app.dstTable], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			}})
	}

	_, err := library.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: requests,
	})
	if err != nil {
		log.Println("Failed to write batch:", err)
		for _, item := range items {
			prettyPrintKey(item, "Failed item", app, true)
		}
	} else {
		prettyPrintKey(lastEvaluatedKey, "Checkpoint", app, false)
	}
}

func clone(app *appConfig, srcTable *dynamodb.DynamoDB, library *ddblibrarian.Library) {
	if app.snapshot != "" {
		err := library.Snapshot(app.snapshot)
		if err != nil {
			log.Fatal("Failed to create snapshot:", err.Error())
		}
	}

	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	for {
		input := &dynamodb.ScanInput{
			TableName: aws.String(app.srcTable),
			// TODO: get as much as possible here and have the writer slicing it
			// TODO: minimize the number of (Get) network calls and parallelize writes
			Limit: aws.Int64(25),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		result, err := srcTable.Scan(input)
		if err != nil {
			// TODO: retry (when useful)
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
				case dynamodb.ErrCodeResourceNotFoundException:
					fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
				case dynamodb.ErrCodeInternalServerError:
					fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(err.Error())
			}
			return
		} else {
			if *result.Count == 0 {
				return
			}
			lastEvaluatedKey = result.LastEvaluatedKey
			go writeItems(result.Items, lastEvaluatedKey, library, app)
		}
	}
}

func prettyPrintKey(item map[string]*dynamodb.AttributeValue, prefix string, app *appConfig, isError bool) {
	dropWhiteSpace := func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}

	format := "%s: %s=%s, %s=%s\n"

	out := os.Stdout
	if isError {
		out = os.Stderr
	}

	fmt.Fprintf(
		out,
		format,
		prefix,
		app.partitionKey,
		strings.Map(dropWhiteSpace, item[app.partitionKey].String()),
		app.rangeKey,
		strings.Map(dropWhiteSpace, item[app.rangeKey].String()),
	)
}

func main() {
	app := &appConfig{}
	// TODO: accept LastEvaluatedKey as a parameter to allow resuming
	flag.StringVar(&app.srcRegion, "source-region", "us-east-1", "AWS region of the source table")
	flag.StringVar(&app.dstRegion, "destination-region", "us-east-1", "AWS region of the destination table")
	flag.StringVar(&app.srcTable, "source", "", "Source DynamoDB table")
	flag.StringVar(&app.dstTable, "destination", "", "Destination DynamoDB table")
	flag.StringVar(&app.partitionKey, "partition-key", "", "Partition key")
	flag.StringVar(&app.partitionKeyType, "partition-key-type", "", "Type of partition key (S or N)")
	flag.StringVar(&app.rangeKey, "range-key", "", "range key")
	flag.StringVar(&app.rangeKeyType, "range-key-type", "", "Type of range key (S or N)")
	flag.StringVar(&app.snapshot, "snapshot", "", "Take a snapshot before starting the copy")

	flag.Parse()
	checkFlags(app)
	srcTable, librarian := connect(app)
	clone(app, srcTable, librarian)
}
