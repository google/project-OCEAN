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

package mailman

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/project-OCEAN/1-raw-data/utils"
)

func TestCreateMailmanFilename(t *testing.T) {
	tests := []struct {
		comparisonType string
		date           string
		want           string
	}{
		{
			comparisonType: "Start zip file name.",
			date:           "1865-06-17",
			want:           "1865-06.mbox.gz",
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			got := createMailmanFilename(test.date)
			if strings.Compare(got, test.want) != 0 {
				t.Errorf("CreateMMFileName response does not match.\n got: %v\nwant: %v", got, test.want)
			}
		})
	}
}

func TestCreateMailManURL(t *testing.T) {
	// TODO test that datetimes entered are in the correct format
	tests := []struct {
		comparisonType string
		url            string
		filename       string
		startDate      string
		endDate        string
	}{
		{
			comparisonType: "Create url",
			url:            "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			filename:       "susan_la_flesche_picotte.mbox.gz",
			startDate:      "1865-06-17",
			endDate:        "1915-09-18",
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			want := fmt.Sprintf("%vexport/python-dev@python.org-%v?start=%v&end=%v", test.url, test.filename, test.startDate, test.endDate)
			got := createMailmanURL(test.url, test.filename, test.startDate, test.endDate)
			if strings.Compare(got, want) != 0 {
				t.Errorf("CreateMMURL response does not match.\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

func TestGetMailmanData(t *testing.T) {
	ctx := context.Background()
	//currentDate := time.Now()

	storage := utils.NewFakeStorageConnection("mailman")

	tests := []struct {
		comparisonType string
		groupName      string
		startDate      string
		endDate        string
		numMonths      int
		wantErr        error
	}{
		{
			comparisonType: "Test StoreInBucket is called and for one month timeframe",
			groupName:      "Susan_La_Flesche_Picotte",
			startDate:      "1915-09-01",
			endDate:        "1915-09-30",
			numMonths:      1,
			wantErr:        storageErr,
		},
		{
			comparisonType: "SetDate error StartDate wrong format",
			groupName:      "Susan_La_Flesche_Picotte",
			startDate:      "06-17",
			endDate:        "1915-09-30",
			numMonths:      1,
			wantErr:        storageErr,
		},
		{
			comparisonType: "Test current date to check for loop issues",
			groupName:      "Katalin Karikó",
			startDate:      "2021-03-31",
			endDate:        "2021-04-01",
			numMonths:      1,
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := GetMailmanData(ctx, storage, test.groupName, test.startDate, test.endDate); !errors.Is(gotErr, test.wantErr) {
				if gotErr == nil || !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("Error doesn't match.\n got: %v\nwant it to contain: %v", gotErr, test.wantErr)
				}
			}
		})
	}
}
