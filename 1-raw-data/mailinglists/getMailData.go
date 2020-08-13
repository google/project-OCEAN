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
// Create tests
// check the most recent file stored and pull only what isn't there
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
	bucketName string
	ctx context.Context
	client *storage.Client
	bucket *storage.BucketHandle
}

func connectCTX() context.Context{
	ctx := context.Background()
	//ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	//defer cancel()
	return ctx
}

func (gcs *GCS) connectGCS() {
	client, err := storage.NewClient(gcs.ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	gcs.client = client

}

func (gcs *GCS) createGCSBucket(projectID string) {
	// Setup client bucket to work from
	gcs.bucket = gcs.client.Bucket(gcs.bucketName)

	buckets := gcs.client.Buckets(gcs.ctx, projectID)
	for {
		attrs, err := buckets.Next()
		// Assuming that if end Iterator then not found and need to create
		if err == iterator.Done {
			// Create bucket if it doesn't exist - https://cloud.google.com/storage/docs/reference/libraries
			if err := gcs.bucket.Create(gcs.ctx, projectID, &storage.BucketAttrs {
				Location:     "US",
			}); err != nil {
				// TODO - add random number to append to bucket name to resolve
				log.Fatalf("Failed to create bucket:", err)
			}
			fmt.Printf("Bucket %v created.\n", gcs.bucketName)
			return
		}
		if err != nil {
			log.Fatalf("Issues setting up Bucket(%q).Objects(): %v. Double check project id.", attrs.Name, err)
		}
		if attrs.Name == gcs.bucketName{
			//getLatestFile() // TODO set this up to check and compare what is in the bucket vs what isn't
			fmt.Printf("Bucket %v exists.\n", gcs.bucketName)
			return
		}
	}
}

func (gcs *GCS) storeGCS(fname string, url string) {
	response, _ := getHTTPResponse(url)
	if response.StatusCode == http.StatusOK {
		obj := gcs.bucket.Object(fname)

		// w implements io.Writer.
		w := obj.NewWriter(gcs.ctx)

		// Copy file into GCS
		_, err := io.Copy(w, response.Body)
		if err != nil {
			log.Fatalf("Failed to copy doc to bucket:", err)
		}
		response.Body.Close()

		// Close, just like writing a file.
		if err := w.Close(); err != nil {
			log.Fatalf("Failed to close:", err)
		}
	}
}

func getHTTPResponse(url string) (*http.Response, error){
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("****** HTTP ERROR *********", err)
	}
	// Defer response.Body.Close() // TODO figure out if this is usable in a func because its closing when passed
	return response, err
}


func getMalingListData(url string, gcs GCS) {
	dom, _ := goquery.NewDocument(url)

	dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
		band, ok := s.Attr("href")
		if ok {
			check := strings.Split(band, ".")
			len := len(check)-1
			if check[len] == "gz" {
				if strings.Split(band, ":")[0] != "https"{
					path := url+band
					//fmt.Printf("Relative path to store is: %s\n", path)
					gcs.storeGCS(band, path)
				}
			}
		}
	})
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
//		fmt.Fprintln(w, attrs.Name)
//		setup.latestFile = attrs.Name
//	}
//}

func main() {

	var mailingListURL string
	var projectID string
	gcs := GCS{ ctx: connectCTX() }

	flag.StringVar(&mailingListURL, "mailinglist-url", "","mailing list url to pull files from")
	flag.StringVar(&projectID, "project-id", "", "project id")
	flag.StringVar(&gcs.bucketName, "bucket", "","bucket name to store files")

	// Parse passed in flags
	flag.Parse()
	gcs.connectGCS()
	gcs.createGCSBucket(projectID)

	getMalingListData(mailingListURL, gcs)
}