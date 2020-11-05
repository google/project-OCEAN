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
	"strings"
)

var (
	httpStrRespErr     = fmt.Errorf("http string")
	clientErr          = fmt.Errorf("client creation")
	createBucketErr    = fmt.Errorf("create bucket")
	emptyBucketName    = fmt.Errorf("empty bucketname")
	emptyFileNameErr   = fmt.Errorf("empty filename")
	storageCtxCloseErr = fmt.Errorf("Failed to close storage connection")
)

type Connection interface {
	StoreContentInBucket(ctx context.Context, fileName, content, source string) (err error)
}

type StorageConnection struct {
	ProjectID  string
	BucketName string
	client     stiface.Client
	bucket     stiface.BucketHandle
}

func (gcs *StorageConnection) ConnectClient(ctx context.Context) (err error) {
	c, err := storage.NewClient(ctx)
	if err != nil {
		err = fmt.Errorf("%w failed: %v", clientErr, err)
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
			err = fmt.Errorf("%w error: %v. Re-enter bucketname.", emptyBucketName, gcs.BucketName)
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
				return fmt.Errorf("%w failed: %v", createBucketErr, err)

			}
			log.Printf("Bucket %v created.\n", gcs.BucketName)
			return
		}
		if err != nil {
			err = fmt.Errorf("%w setup issues error: %v. Double check project id.", createBucketErr, err)
			return
		}
		if attrs.Name == gcs.BucketName {
			//getLatestFile() // TODO set this up to check and compare what is in the bucket vs what isn't
			log.Printf("Bucket %v exists.\n", gcs.BucketName)
			return
		}
	}
}

// Store url content in storage.
func (gcs *StorageConnection) StoreContentInBucket(ctx context.Context, fileName, content, source string) (err error) {
	var response *http.Response
	//TODO add more filename validation
	if fileName == "" {
		return fmt.Errorf("%w", emptyFileNameErr)
	}
	obj := gcs.bucket.Object(fileName)

	// w implements io.Writer.
	w := obj.NewWriter(ctx)

	if source == "url" {
		// Get HTTP response
		response, err = http.Get(content)
		if err != nil {
			return fmt.Errorf("%w response error: %v", httpStrRespErr, err)
		}
		defer response.Body.Close()

		if response.StatusCode == http.StatusOK {
			// Copy file into storage
			_, err = io.Copy(w, response.Body)

		} else if source == "text" {

			// Copy file into storage
			_, err = io.Copy(w, strings.NewReader(content))
		}
	}
	if err != nil {
		// Note not breaking when a file does not load but logging to investigate.
		log.Printf("Storage did not copy %v to bucket with the error: %v", fileName, err)
	}

	if err = w.Close(); err != nil {
		return fmt.Errorf("%w: %v", storageCtxCloseErr, err)
	}

	return
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gcs := StorageConnection{}

	if err := gcs.ConnectClient(ctx); err != nil {
		log.Fatalf("GCS connection failed: %v", err)
	}

	if err := gcs.CreateBucket(ctx); err != nil {
		log.Fatalf("%v", err)
	}
}
