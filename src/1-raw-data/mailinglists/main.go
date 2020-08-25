// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

//TODO
// Check the most recent file stored and pull only what isn't there

import (
	"cloud.google.com/go/storage"
	"context"
	"flag"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"net/http"
)

type GCS struct {
	ctx    context.Context
	client *storage.Client
	bucket *storage.BucketHandle
}

var (
	projectID      = flag.String("project-id", "", "project id")
	bucketName     = flag.String("bucket-name", "", "bucket name to store files")
	mailingList	   = flag.String("mailinglist", "piper", "Choose which mailing list to process either piper (default), mailman")
	mailingListURL = flag.String( "mailinglist-url", "", "mailing list url to pull files from")
	startDate 	   = flag.String("start-date", "", "Start date in format of year-month-date and 4dig-2dig-2dig")
	endDate	       = flag.String( "end-date", "", "End date in format of year-month-date and 4dig-2dig-2dig")
	gcs            = GCS{}
)

func (gcs *GCS) connectCTX() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	return context.WithCancel(ctx)
}

func (gcs *GCS) connectGCS() error {
	if client, err := storage.NewClient(gcs.ctx); err != nil {
		return fmt.Errorf("Failed to create client: %v", err)
	} else {
		gcs.client = client
		return nil
	}
}

func (gcs *GCS) createGCSBucket() error {
	// Setup client bucket to work from
	gcs.bucket = gcs.client.Bucket(*bucketName)

	buckets := gcs.client.Buckets(gcs.ctx, *projectID)
	for {
		attrs, err := buckets.Next()
		// Assume that if Iterator end then not found and need to create bucket
		if err == iterator.Done {
			// Create bucket if it doesn't exist - https://cloud.google.com/storage/docs/reference/libraries
			if err := gcs.bucket.Create(gcs.ctx, *projectID, &storage.BucketAttrs{
				Location: "US",
			}); err != nil {
				// TODO - add random number to append to bucket name to resolve
				return fmt.Errorf("Failed to create bucket: %v", err)
			}
			log.Printf("Bucket %v created.\n", *bucketName)
			return nil
		}
		if err != nil {
			return fmt.Errorf("Issues setting up Bucket(%q).Objects(): %v. Double check project id.", attrs.Name, err)
		}
		if attrs.Name == *bucketName {
			//getLatestFile() // TODO set this up to check and compare what is in the bucket vs what isn't
			log.Printf("Bucket %v exists.\n", bucketName)
			return nil
		}
	}
}

func (gcs *GCS) storeGCS(fileName string, url string) {
	// Get HTTP response
	response, _ := http.Get(url)
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		obj := gcs.bucket.Object(fileName)

		// w implements io.Writer.
		w := obj.NewWriter(gcs.ctx)

		// Copy file into GCS
		_, err := io.Copy(w, response.Body)
		if err != nil {
			log.Printf("Failed to copy %v to bucket: %v", fileName, err)
		}
		response.Body.Close()

		// Close, just like writing a file.
		if err := w.Close(); err != nil {
			log.Fatalf("Failed to close: %v", err)
		}
	}
}

func main() {
	flag.Parse()

	ctx, cancel := gcs.connectCTX()
	defer cancel()
	gcs.ctx = ctx

	if err := gcs.connectGCS(); err != nil {
		log.Fatalf("Connect GCS failes: %v", err)
	}

	if err := gcs.createGCSBucket(); err != nil {
		log.Fatalf("Create GCS Bucket failed: %v", err)
	}

	switch *mailingList {
	case "piper":
		piperMailMain()
	case "mailman":
		mailManMain()
	default:
		log.Fatalf("Mailing list %v is not an option. Change the option submitted.: ", mailingList)
	}
}

