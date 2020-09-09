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

/*
This package is for loading different mailing list data types into Cloud Storage.
*/

package gcs

//TODO
// Check the most recent file stored and pull only what isn't there

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"net/http"
)

type Connection interface {
	StoreInBucket(ctx context.Context, fileName, url string) error
}

type StorageConnection struct {
	ProjectID  string
	BucketName string
	client     stiface.Client
	bucket     stiface.BucketHandle
}

func (gcs *StorageConnection) ConnectClient(ctx context.Context) (err error){
	c, err := storage.NewClient(ctx)
	if err != nil {
		err = fmt.Errorf("Failed to create client: %v", err)
		return
	}
	client := stiface.AdaptClient(c)
	gcs.client = client
	return
}

// Creates storage bucket if it doesn't exist.
func (gcs *StorageConnection) CreateBucket(ctx context.Context) (err error) {
	var attrs *storage.BucketAttrs
	// Setup client bucket to work from
	gcs.bucket = gcs.client.Bucket(gcs.BucketName)

	buckets := gcs.client.Buckets(ctx, gcs.ProjectID)
	for {
		// TODO bucket name validation
		if gcs.BucketName == "" {
			err = fmt.Errorf("BucketName entered is empty %v. Re-enter.", gcs.BucketName)
			return
		}
		attrs, err = buckets.Next()
		// Assume that if Iterator end then not found and need to create bucket
		if err == iterator.Done {
			// Create bucket if it doesn't exist - https://cloud.google.com/storage/docs/reference/libraries
			if err = gcs.bucket.Create(ctx, gcs.ProjectID, &storage.BucketAttrs{
				Location: "US",
			}); err != nil {
				// TODO - add random number to append to bucket name to resolve
				return fmt.Errorf("Failed to create bucket: %v", err)

			}
			log.Printf("Bucket %v created.\n", gcs.BucketName)
			return
		}
		if err != nil {
			err = fmt.Errorf("Issues setting up Bucket: %q due to error: %w. Double check project id.", attrs.Name, err)
			return
		}
		if attrs.Name == gcs.BucketName {
			//getLatestFile() // TODO set this up to check and compare what is in the bucket vs what isn't
			log.Printf("Bucket %v exists.\n", gcs.BucketName)
			return
		}
	}
}

// Store files in storage.
func (gcs *StorageConnection) StoreInBucket(ctx context.Context, fileName, url string) (err error) {
	var response *http.Response
	//TODO add more filename validation
	if fileName == "" {
		return fmt.Errorf("Filename is empty.")
	}
	// Get HTTP response
	response, err = http.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP response returned an error: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		obj := gcs.bucket.Object(fileName)

		// w implements io.Writer.
		w := obj.NewWriter(ctx)

		// Copy file into storage
		_, err = io.Copy(w, response.Body)
		if err != nil {
			log.Printf("Failed to copy %v to bucket with the error: %v", fileName, err)
		}

		if err = w.Close(); err != nil {
			return fmt.Errorf("Failed to close storage connection: %v", err)
		}
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gcs := StorageConnection{}

	if err := gcs.ConnectClient(ctx); err != nil {
		log.Fatalf("Connect GCS failes: %v", err)
	}

	if err := gcs.CreateBucket(ctx); err != nil {
		log.Fatalf("Create GCS Bucket failed: %v", err)
	}
}
