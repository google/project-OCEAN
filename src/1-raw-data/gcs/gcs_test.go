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

package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"strings"
	"testing"
)

type fakeClient struct {
	stiface.Client
	buckets map[string]*fakeBucket
}

type fakeBucket struct {
	attrs   *storage.BucketAttrs
	objects map[string][]byte
}

func newFakeClient() stiface.Client {
	return &fakeClient{buckets: map[string]*fakeBucket{}}
}

func (c *fakeClient) Bucket(name string) stiface.BucketHandle {
	return fakeBucketHandle{c: c, name: name}
}

type fakeBucketHandle struct {
	stiface.BucketHandle
	c    *fakeClient
	name string
}

func setupGCS(t *testing.T) *StorageConnection {
	ctx := context.Background()
	bucketName := "susan"

	c, err := storage.NewClient(ctx)
	if err != nil {
		t.Errorf("Failed to simulate client: %v", err)
	}
	client := newFakeClient() // stiface as part of storage
	defer client.Close()
	//bucketHandle := client.Bucket(bucketName)

	gcs := &StorageConnection{
		Ctx:        ctx,
		ProjectID:  "susan-la-flesche-picotte",
		BucketName: bucketName,
		//client: client,
		//bucket: bucketHandle,
	}
	return gcs
}

func TestCreateGCSBucket(t *testing.T) {
	gcs := setupGCS(t)

	tests := []struct {
		comparisonType string
		bucketName     string
		storage        StorageConnection
		wantErr        error
	}{
		// Test no error
		{
			comparisonType: "Test nil error",
			storage:        *gcs,
			bucketName:     "susan",
			wantErr:        nil,
		},
		// Test empty bucket name
		{
			comparisonType: "Test not nil error",
			storage:        *gcs,
			bucketName:     "",
			wantErr:        fmt.Errorf("06-17"),
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			test.storage.BucketName = test.bucketName
			if gotErr := gcs.CreateGCSBucket(); gotErr != test.wantErr {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("CreateMMFileName response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
				}
			}
		})
	}

}

func TestStoreGCS(t *testing.T) {
	gcs := setupGCS(t)
	tests := []struct {
		comparisonType string
		storage        StorageConnection
		filename       string
		url            string
		wantErr        error
	}{
		// Test no error
		{
			comparisonType: "Test nil error",
			storage:        *gcs,
			filename:       "picoatte.gz",
			url:            "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			wantErr:        nil,
		},
		// Test empty filename
		{
			comparisonType: "Test not nil error",
			storage:        *gcs,
			filename:       "",
			url:            "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			wantErr:        fmt.Errorf("06-17"),
		},
		// Test empty url
		{
			comparisonType: "Test not nil error",
			storage:        *gcs,
			filename:       "picoatte.gz",
			url:            "",
			wantErr:        fmt.Errorf("06-17"),
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := gcs.StoreGCS(test.filename, test.url); gotErr != test.wantErr {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("CreateMMFileName response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
				}
			}
		})
	}

}
