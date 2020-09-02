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
	"1-raw-data/gcs"
	"context"
	"strings"
	"testing"
)

// Simulate Storage Connection struct
type StorageConnection struct {
	ProjectID  string
	BucketName string
}

// Simulate StoreGCS
func (gcs *StorageConnection) StoreGCS(t *testing.T, fileName, url string) error {
	return nil
}

func TestGetPipermailData(t *testing.T) {
	ctx := context.Background()
	storage := gcs.StorageConnection{}

	tests := []struct {
		comparisonType string
		storage        gcs.StorageConnection
		mailingListURL string
		wantErr        error
	}{
		{
			comparisonType: "Test nil error",
			storage:        storage,
			mailingListURL: "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			wantErr:        nil,
		},
		{
			comparisonType: "Test not nil error",
			storage:        storage,
			mailingListURL: "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			wantErr:        nil,
		},
	}

	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := GetPipermailData(ctx, test.storage, test.mailingListURL); gotErr != test.wantErr {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("GetPipermailData response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
				}
			}
		})
	}
}
