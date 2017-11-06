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
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/marcoalmeida/ddblibrarian"
)

// Create a new session for the DynamoDB service, create a new instance of ddblibrarian (for some existing table,
// passing along the session), and print out all existing snapshots as an example of basic usage.
//
// Note: error handling has been greatly simplified. Make sure not to just copy-past this to a live, production system.
func ExampleLibrary_ListSnapshots() {
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
	library, err := ddblibrarian.New("example", "year", "N", "", "", s)
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
