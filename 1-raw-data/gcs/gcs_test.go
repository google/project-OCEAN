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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"
)

type fakeClient struct {
	stiface.Client
	buckets map[string]*fakeBucket
}

type fakeBucketHandle struct {
	stiface.BucketHandle
	c    *fakeClient
	name string
}

type fakeBucket struct {
	attrs   *storage.BucketAttrs
	objects map[string][]byte
}

type fakeBucketIterator struct {
	stiface.BucketIterator
	i    int
	next []storage.BucketAttrs
}

type fakeObjectHandle struct {
	stiface.ObjectHandle
	c          *fakeClient
	bucketName string
	name       string
}

func newFakeClient() stiface.Client {
	return &fakeClient{buckets: map[string]*fakeBucket{}}
}

func (b fakeBucketHandle) Object(name string) stiface.ObjectHandle {
	return fakeObjectHandle{c: b.c, bucketName: b.name, name: name}
}

func (c *fakeClient) Bucket(name string) stiface.BucketHandle {
	return &fakeBucketHandle{c: c, name: name}
}

type fakeWriter struct {
	stiface.Writer
	obj fakeObjectHandle
	buf bytes.Buffer
}

func (o fakeObjectHandle) NewWriter(context.Context) stiface.Writer {
	return &fakeWriter{obj: o}
}

func (w *fakeWriter) Write(data []byte) (int, error) {
	return w.buf.Write(data)
}

func (w *fakeWriter) Close() error {
	return nil
}

func (c *fakeClient) Buckets(ctx context.Context, projectID string) (it stiface.BucketIterator) {
	switch projectID {
	case "Environmentalist":
		it = &fakeBucketIterator{
			i: 0,
			next: []storage.BucketAttrs{
				{Name: "Economist"},
			},
		}
	case "Economist":
		it = &fakeBucketIterator{
			i: 0,
			next: []storage.BucketAttrs{
				{Name: "Environmentalist"},
				{Name: "Economist"},
			},
		}
	case "":
		it = &fakeBucketIterator{
			i: 0,
			next: []storage.BucketAttrs{
				{Name: "Environmentalist"},
			},
		}
	}
	return
}

func (it *fakeBucketIterator) Next() (a *storage.BucketAttrs, err error) {
	if it.i == len(it.next) {
		err = iterator.Done
		return
	}

	a = &it.next[it.i]
	it.i += 1
	return
}

// TODO assert the name was passed - don't need to have the rest of this
func (b fakeBucketHandle) Create(_ context.Context, _ string, attrs *storage.BucketAttrs) error {
	if _, ok := b.c.buckets[b.name]; ok {
		return fmt.Errorf("bucket %q already exists", b.name)
	}
	if attrs == nil {
		attrs = &storage.BucketAttrs{}
	}
	attrs.Name = b.name
	b.c.buckets[b.name] = &fakeBucket{attrs: attrs, objects: map[string][]byte{}}
	return nil
}

func setupGCS(t *testing.T) *StorageConnection {
	bucketName := "LaDuke"
	client := newFakeClient()
	bucketHandle := client.Bucket(bucketName)

	gcs := &StorageConnection{
		ProjectID:  "Winona-LaDuke",
		BucketName: bucketName,
		client:     client,
		bucket:     bucketHandle,
	}
	return gcs
}

func TestCreateBucket(t *testing.T) {
	ctx := context.Background()
	gcs := setupGCS(t)

	tests := []struct {
		comparisonType string
		bucketName     string
		storage        *StorageConnection
		projectID      string
		wantErr        error
	}{
		{
			comparisonType: "Test Create bucket is called",
			storage:        gcs,
			bucketName:     "Environmentalist",
			projectID:      "Environmentalist",
			wantErr:        nil,
		},
		{
			comparisonType: "Test Create bucket is not called",
			storage:        gcs,
			bucketName:     "Environmentalist",
			projectID:      "Economist",
			wantErr:        nil,
		},
		{
			comparisonType: "Test empty bucket name and error",
			storage:        gcs,
			bucketName:     "",
			projectID:      "",
			wantErr:        emptyBucketName,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			test.storage.BucketName = test.bucketName
			test.storage.ProjectID = test.projectID
			if gotErr := gcs.CreateBucket(ctx); !errors.Is(gotErr, test.wantErr) {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("CreateMMFileName response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
				}
			}
		})
	}
}

//func TestStoreContentInBucket(t *testing.T) {
//	ctx := context.Background()
//	gcs := setupGCS(t)
//
//	var (
//		gotVerify int64
//		gotErr    error
//	)
//
//	tests := []struct {
//		comparisonType string
//		storage        *StorageConnection
//		filename       string
//		content        string
//		source         string
//		wantResponse   bool
//		wantErr        error
//	}{
//		{
//			comparisonType: "Test Store called without error on url content",
//			storage:        gcs,
//			filename:       "environmentalist.gz",
//			content:        "https://en.wikipedia.org/wiki/Winona_LaDuke",
//			source:         "url",
//			wantResponse:   true,
//			wantErr:        nil,
//		},
//		{
//			comparisonType: "Test empty filename error",
//			storage:        gcs,
//			filename:       "",
//			content:        "https://en.wikipedia.org/wiki/Winona_LaDuke",
//			source:         "url",
//			wantResponse:   false,
//			wantErr:        emptyFileNameErr,
//		},
//		{
//			comparisonType: "Test empty url",
//			storage:        gcs,
//			filename:       "economist.gz",
//			content:        "",
//			source:         "url",
//			wantResponse:   false,
//			wantErr:        httpStrRespErr,
//		},
//		{
//			comparisonType: "Test Store called without error on text content",
//			storage:        gcs,
//			filename:       "writer.gz",
//			content:        "An American environmentalist, economist, writer who found the Indigenous Women's Network.",
//			source:         "text",
//			wantResponse:   true,
//			wantErr:        nil,
//		},
//	}
//	for _, test := range tests {
//		t.Run(test.comparisonType, func(t *testing.T) {
//			if gotVerify, gotErr = gcs.StoreContentInBucket(ctx, test.filename, test.content, test.source); !errors.Is(gotErr, test.wantErr) {
//				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
//					t.Errorf("StoreContentInBucket response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
//				}
//			}
//			if test.wantResponse != (gotVerify > 0) {
//				t.Errorf("Storage Copy did not perform as expected. Returned value got: %v which does not match what was expected.", gotVerify)
//			}
//		})
//	}
//
//}
