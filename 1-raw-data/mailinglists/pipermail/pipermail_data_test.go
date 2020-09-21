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

package pipermail

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

type fakeStorageConnection struct {
	gcs.StorageConnection
	ProjectID  string
	BucketName string
}

func newFakeStorageConnection() *fakeStorageConnection {
	return &fakeStorageConnection{ProjectID: "pine_leaf", BucketName: "Bíawacheeitchish"}
}

// Simulate StoreGCS
func (gcs *fakeStorageConnection) StoreInBucket(ctx context.Context, fileName, url string) (storageErr error) {
	errStorageInBucket := errors.New("gcs storage failed")
	if strings.Contains(url, "pipermail") {
		storageErr = fmt.Errorf("%w: %v", errStorageInBucket, os.ErrNotExist)
	}
	return
}

func TestGetPipermailData(t *testing.T) {
	ctx := context.Background()
	storage := newFakeStorageConnection()

	tests := []struct {
		comparisonType string
		gcs            *fakeStorageConnection
		mailingListURL string
		wantErr        error
	}{
		{
			comparisonType: "Test url is not pipermail and doesn't store",
			gcs:            storage,
			mailingListURL: "https://en.wikipedia.org/Pine_Leaf",
			wantErr:        nil,
		},
		{
			comparisonType: "Test pipermail url gets to StoreGCS method and erro",
			gcs:            storage,
			mailingListURL: "https://mail.python.org/pipermail/python-announce-list/",
			wantErr:        os.ErrNotExist,
		},
	}

	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := GetPipermailData(ctx, test.gcs, test.mailingListURL); !errors.Is(gotErr, test.wantErr) {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("GetPipermailData response does not match.\n got: %v\nwant: %v", errors.Unwrap(gotErr), test.wantErr)
				}
			}
		})
	}
}
