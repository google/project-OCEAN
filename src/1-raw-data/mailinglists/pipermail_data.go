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
// Add test of the specific page format expected and how to parse it
// Check the most recent file stored and pull only what isn't there
// Run this monthly at start of new month to pull all new data

import (
	"cloud.google.com/go/storage"
	"context"
	"flag"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"net/http"
	"strings"
)

type GCS struct {
	ctx    context.Context
	client *storage.Client
	bucket *storage.BucketHandle
}

var (
	mailingListURL string
	projectID      string
	bucketName     string
	gcs            = GCS{}
)

func connectCTX() (context.Context, context.CancelFunc) {
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
	gcs.bucket = gcs.client.Bucket(bucketName)

	buckets := gcs.client.Buckets(gcs.ctx, projectID)
	for {
		attrs, err := buckets.Next()
		// Assume that if Iterator end then not found and need to create bucket
		if err == iterator.Done {
			// Create bucket if it doesn't exist - https://cloud.google.com/storage/docs/reference/libraries
			if err := gcs.bucket.Create(gcs.ctx, projectID, &storage.BucketAttrs{
				Location: "US",
			}); err != nil {
				// TODO - add random number to append to bucket name to resolve
				return fmt.Errorf("Failed to create bucket: %v", err)
			}
			log.Printf("Bucket %v created.\n", bucketName)
			return nil
		}
		if err != nil {
			return fmt.Errorf("Issues setting up Bucket(%q).Objects(): %v. Double check project id.", attrs.Name, err)
		}
		if attrs.Name == bucketName {
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

func getMailingListData() {
	response, _ := http.Get(mailingListURL)
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		dom, _ := goquery.NewDocumentFromReader(response.Body)
		dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
			band, ok := s.Attr("href")
			if ok {
				check := strings.Split(band, ".")
				len := len(check) - 1
				if check[len] == "gz" {
					if strings.Split(band, ":")[0] != "https" {
						path := mailingListURL + band
						gcs.storeGCS(band, path)
					}
				}
			}
		})
	}
}

// TODO create func to create map of what is in bucket and then compare to what is pulled from site so only pull new files
//func getLatestFile(setup Setup){
//	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
//	defer cancel()
//
//	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
//		Prefix:    prefix,
//		Delimiter: delim,
//	})
//	for {
//		attrs, err := it.Next()
//		if err == iterator.Done {
//			break
//		}
//		if err != nil {
//			return fmt.Errorf("Bucket(%q).Objects(): %v", bucket, err)
//		}
//		log.Fprintln(w, attrs.Name)
//		setup.latestFile = attrs.Name
//	}
//}

func main() {

	// Parse passed in flags
	flag.StringVar(&bucketName, "bucket-name", "", "bucket name to store files")
	flag.StringVar(&mailingListURL, "mailinglist-url", "", "mailing list url to pull files from")
	flag.StringVar(&projectID, "project-id", "", "project id")
	flag.Parse()

	ctx, cancel := connectCTX()
	defer cancel()
	gcs.ctx = ctx

	if err := gcs.connectGCS(); err != nil {
		log.Fatal("Connect GCS failes: %v", err)
	}

	if err := gcs.createGCSBucket(); err != nil {
		log.Fatal("Create GCS Bucket failed: %v", err)
	}

	getMailingListData()
}
