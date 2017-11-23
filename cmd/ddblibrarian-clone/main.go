package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/marcoalmeida/ddblibrarian"
)

const (
	maxRetries int = 3
	batchSize  int = 25
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

func writeBatch(
	batch map[string][]*dynamodb.WriteRequest,
	library *ddblibrarian.Library,
) error {
	var err error

	for i := 0; i < maxRetries; i++ {
		_, err = library.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException {
					wait := math.Pow(2, float64(i)) * 100
					log.Printf("BatchWriteItem: backing off for %f milliseconds\n", wait)
					time.Sleep(time.Duration(wait) * time.Millisecond)
					continue
				}
			} else {
				// there's no point on retrying
				return err
			}
		} else {
			// the write succeeded, nothing else to do here
			return nil
		}
	}

	// if we've made it this far, all attempts have failed
	return err
}

func writeItems(
	items []map[string]*dynamodb.AttributeValue,
	lastEvaluatedKey map[string]*dynamodb.AttributeValue,
	library *ddblibrarian.Library,
	app *appConfig,
) {
	var err error = nil
	requests := make(map[string][]*dynamodb.WriteRequest, 0)

	// create groups of 25 items -- max batch size
	for i, item := range items {
		if (i%batchSize) == 0 && i > 0 {
			err = writeBatch(requests, library)
			if err != nil {
				break
			}
			requests = make(map[string][]*dynamodb.WriteRequest, 0)
		} else {
			requests[app.dstTable] = append(requests[app.dstTable], &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: item,
				}})
		}
	}

	if err != nil {
		log.Fatalln("Failed to write batch:", err)
		// this can be quite long...
		//for _, item := range items {
		//	prettyPrintKey(item, "Failed item", app, true)
		//}
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
			TableName:      aws.String(app.srcTable),
			ConsistentRead: aws.Bool(true),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		for i := 0; i < maxRetries; i++ {
			result, err := srcTable.Scan(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					if aerr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException {
						wait := math.Pow(2, float64(i)) * 100
						log.Printf("Scan: backing off for %f milliseconds\n", wait)
						time.Sleep(time.Duration(wait) * time.Millisecond)
						continue
					}
				} else {
					// there's no point on retrying
					log.Fatalln("Scan: failed after", maxRetries, ":", err)
				}
			} else {
				// we're done
				if *result.Count == 0 {
					return
				}
				// save
				lastEvaluatedKey = result.LastEvaluatedKey
				go writeItems(result.Items, lastEvaluatedKey, library, app)
			}
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
