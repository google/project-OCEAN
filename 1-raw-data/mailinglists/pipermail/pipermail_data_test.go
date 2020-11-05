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
	"strings"
	"testing"

	"github.com/google/project-OCEAN/1-raw-data/utils"
)

func TestGetPipermailData(t *testing.T) {
	ctx := context.Background()
	storage := utils.NewFakeStorageConnection("pipermail")

	tests := []struct {
		comparisonType string
		gcs            *utils.FakeStorageConnection
		groupName      string
		httpToDom      utils.HttpDomeResponse
		wantErr        error
	}{
		{
			comparisonType: "Test Storage called and no error",
			gcs:            storage,
			groupName:      "Pine-Leaf",
			httpToDom:      utils.FakeHttpDomResponse,
			wantErr:        nil,
		},
		{
			comparisonType: "Test Storage called and returns error",
			gcs:            storage,
			groupName:      "Missing",
			httpToDom:      utils.FakeHttpDomResponse,
			wantErr:        storageErr,
		},
	}

	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := GetPipermailData(ctx, test.gcs, test.groupName, test.httpToDom); !errors.Is(gotErr, test.wantErr) {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("GetPipermailData response does not match.\n got: %v\nwant: %v", errors.Unwrap(gotErr), test.wantErr)
				}
			}
		})
	}
}
