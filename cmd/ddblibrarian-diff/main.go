package main

import (
	"flag"
	"log"

	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/marcoalmeida/ddblibrarian"
	"math"
	"reflect"
	"time"
)

const (
	defaultMaxRetries int = 3
	batchSize         int = 25
)

type appConfig struct {
	region           [2]string
	table            [2]string
	partitionKey     string
	partitionKeyType string
	rangeKey         string
	rangeKeyType     string
	snapshot         [2]string
	maxRetries       int
	consistentRead   bool
}

// because a modern language like Go does not need to support optional/required flags...
func checkFlags(app *appConfig) {
	if app.table[0] == "" || app.table[1] == "" {
		log.Fatal("Need both DynamoDB tables")
	}

	if app.partitionKey == "" {
		log.Fatal("The partition key is required")
	}

	if app.partitionKeyType == "" {
		log.Fatal("The partition key type (S or N) is required")
	}
}

func connect(app *appConfig) []*ddblibrarian.Library {
	ddbSession1, err := session.NewSession(&aws.Config{
		Region:     aws.String(app.region[0]),
		MaxRetries: aws.Int(app.maxRetries),
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	ddbSession2, err := session.NewSession(&aws.Config{
		Region:     aws.String(app.region[1]),
		MaxRetries: aws.Int(app.maxRetries),
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	client1, err := ddblibrarian.New(
		app.table[0],
		app.partitionKey,
		app.partitionKeyType,
		app.rangeKey,
		app.rangeKeyType,
		ddbSession1,
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	client2, err := ddblibrarian.New(
		app.table[1],
		app.partitionKey,
		app.partitionKeyType,
		app.rangeKey,
		app.rangeKeyType,
		ddbSession2,
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	return []*ddblibrarian.Library{client1, client2}
}

// make sure every element in data (from src table) exists in the dst table
func contain(scannedData *dynamodb.ScanOutput, dst int, library []*ddblibrarian.Library, app *appConfig) error {
	requests := make([]map[string]*dynamodb.AttributeValue, 0)

	// create groups of 25 items -- max batch size
	for i, item := range scannedData.Items {
		if (i%batchSize) == 0 && i > 0 {
			input := &dynamodb.BatchGetItemInput{
				RequestItems: map[string]*dynamodb.KeysAndAttributes{
					app.table[dst]: {
						Keys: requests,
					},
				}}
			output, err := library[dst].BatchGetItem(input)
			if err != nil {
				return err
			}

			// print all items in data that are not in output
			// compare content
			if len(output.Responses[app.table[dst]]) != len(requests) {
				for j := 0; j < len(requests); j++ {
					exists := false
					// we need the whole row (not just the primary key)
					e1 := scannedData.Items[i-j]
					fmt.Println("LOOKING INTO", i-j)
					for _, e2 := range output.Responses[app.table[dst]] {
						if reflect.DeepEqual(e1, e2) {
							exists = true
							break
						}
					}
					if !exists {
						log.Println("Item not found", e1, "in", output.Responses[app.table[dst]])
					}
				}
			} else {
				log.Println("ALL GOOD", i)
			}

			requests = make([]map[string]*dynamodb.AttributeValue, 0)
		} else {
			foo := map[string]*dynamodb.AttributeValue{}

			if app.partitionKeyType == "S" {
				foo[app.partitionKey] = &dynamodb.AttributeValue{S: item[app.partitionKey].S}
			} else {
				foo[app.partitionKey] = &dynamodb.AttributeValue{N: item[app.partitionKey].N}
			}
			if app.rangeKey != "" {
				if app.rangeKeyType == "S" {
					foo[app.rangeKey] = &dynamodb.AttributeValue{S: item[app.rangeKey].S}
				} else {
					foo[app.rangeKey] = &dynamodb.AttributeValue{N: item[app.rangeKey].N}
				}
			}

			requests = append(requests, foo)
		}
	}

	// check remaining
	log.Println("DONE HERE")
	return nil
}

func diff(fst, snd int, library []*ddblibrarian.Library, app *appConfig) {
	i := 0
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(app.table[fst]),
			ConsistentRead: aws.Bool(app.consistentRead),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		for i := 0; i < app.maxRetries; i++ {
			result, err := library[fst].ScanFromSnapshot(input, app.snapshot[fst])
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
					log.Fatalln("Scan: failed after", app.maxRetries, ":", err)
				}
			} else {
				lastEvaluatedKey = result.LastEvaluatedKey
				// async? depends on good rate limiting
				err := contain(result, snd, library, app)
				if err != nil {
					log.Fatalln("Failed to compare batch:", err)
				}
				// no need to retry again
				break
			}
		}
		// we're done
		if len(lastEvaluatedKey) == 0 {
			return
		}

		log.Println("Checkpoint:", i)
		i++
	}
}

func main() {
	app := &appConfig{}

	flag.StringVar(&app.region[0], "region1", "us-east-1", "AWS region of the first DynamoDB table")
	flag.StringVar(&app.region[1], "region2", "us-east-1", "AWS region of the second DynamoDB table")
	flag.StringVar(&app.table[0], "table1", "", "First DynamoDB table")
	flag.StringVar(&app.table[1], "table2", "", "Second DynamoDB table")
	flag.StringVar(&app.partitionKey, "partition-key", "", "Partition key")
	flag.StringVar(&app.partitionKeyType, "partition-key-type", "", "Type of partition key (S or N)")
	flag.StringVar(&app.rangeKey, "range-key", "", "range key")
	flag.StringVar(&app.rangeKeyType, "range-key-type", "", "Type of range key (S or N)")
	flag.StringVar(
		&app.snapshot[0],
		"snapshot1",
		"",
		"Snapshot to read on table1. Empty for the whole table",
	)
	flag.StringVar(
		&app.snapshot[1],
		"snapshot2",
		"",
		"Snapshot to read on table2. Empty for the whole table",
	)
	flag.IntVar(
		&app.maxRetries,
		"max-retries",
		defaultMaxRetries,
		"Maximum number of retries (with exponential backoff)",
	)
	flag.BoolVar(&app.consistentRead, "consistent-read", true, "Use a strong consistency model")

	flag.Parse()

	checkFlags(app)

	library := connect(app)
	diff(0, 1, library, app)
	diff(1, 0, library, app)
}
