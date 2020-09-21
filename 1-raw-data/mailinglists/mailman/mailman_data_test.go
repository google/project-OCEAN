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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

type fakeStorageConnection struct {
	gcs.StorageConnection
	ProjectID  string
	BucketName string
}

func newFakeStorageConnection() *fakeStorageConnection {
	return &fakeStorageConnection{ProjectID: "Susan-Picotte", BucketName: "Physician"}
}

// Simulate gcs StoreInBucket
func (gcs *fakeStorageConnection) StoreInBucket(ctx context.Context, fileName, url string) (storageErr error) {
	if strings.Contains(url, "Susan") {
		err := os.ErrNotExist
		storageErr = fmt.Errorf("%v", err)
	}
	return
}

func TestSetDates(t *testing.T) {
	// Test passing in empty start, empty date, same date, start older than end, not a string
	today := time.Now().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	tests := []struct {
		comparisonType string
		start          string
		end            string
		wantStart      string
		wantEnd        string
		err            error
	}{
		{
			comparisonType: "Dates empty\n",
			wantStart:      yesterday,
			wantEnd:        today,
			err:            nil,
		},
		{
			comparisonType: "Start date empty\n",
			end:            "1915-09-18",
			wantStart:      yesterday,
			wantEnd:        "1915-09-18",
			err:            fmt.Errorf(today),
		},
		{
			comparisonType: "End date empty\n",
			start:          "1865-06-17",
			wantStart:      "1865-06-17",
			wantEnd:        today,
			err:            nil,
		},
		{
			comparisonType: "Start and end dates provided and correct\n",
			start:          "1865-06-17",
			end:            "1915-09-18",
			wantStart:      "1865-06-17",
			wantEnd:        "1915-09-18",
			err:            nil,
		},
		// Test end date after today's date if start of month
		{
			comparisonType: "End date after today",
			start:          "2020-09-01",
			end:            "3020-09-01",
			wantStart:      "2020-09-01",
			wantEnd:        today,
			err:            nil,
		},
		{
			comparisonType: "End date after today",
			start:          "2020-09-01",
			end:            "3020-09-30",
			wantStart:      "2020-09-01",
			wantEnd:        today,
			err:            nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotStart, gotEnd, err := setDates(test.start, test.end); errors.Is(err, test.err) {
				if strings.Compare(gotStart.Format("2006-01-02"), test.wantStart) != 0 {
					t.Errorf("SetDates start response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotStart.Format("2006-01-02"), test.wantStart)
				}
				if strings.Compare(gotEnd.Format("2006-01-02"), test.wantEnd) != 0 {
					t.Errorf("SetDates end response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotEnd.Format("2006-01-02"), test.wantEnd)
				}
			} else if !strings.Contains(err.Error(), test.err.Error()) {
				t.Errorf("Expected error mismatch for %v.\n got: %v\nwant it to contain: %v", test.comparisonType, err, test.err)
			}
		})
	}
}

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

func TestBreakDateByMonth(t *testing.T) {
	var startDateTime, endDateTime time.Time

	tests := []struct {
		comparisonType string
		start          string
		end            string
		wantStart      string
		wantEnd        string
	}{
		{
			comparisonType: "One month.\n",
			start:          "1915-09-01",
			end:            "1915-09-30",
			wantStart:      "1915-09-01",
			wantEnd:        "1915-10-01",
		},
		{
			comparisonType: "Start not 1st and end over a month after\n",
			start:          "1865-06-17",
			end:            "1915-09-30",
			wantStart:      "1865-06-01",
			wantEnd:        "1865-07-01",
		},
		{
			comparisonType: "Start is 1st and end over a month after\n",
			start:          "1865-07-01",
			end:            "1915-09-01",
			wantStart:      "1865-07-01",
			wantEnd:        "1865-08-01",
		},
		{
			comparisonType: "End is not the 1st of the following month\n",
			start:          "1865-07-01",
			end:            "1865-07-18",
			wantStart:      "1865-07-01",
			wantEnd:        "1865-08-01",
		},
		{
			comparisonType: "Check Feb\n",
			start:          "1865-02-01",
			end:            "1865-03-18",
			wantStart:      "1865-02-01",
			wantEnd:        "1865-03-01",
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			startDateTime, _ = time.Parse("2006-01-02", test.start)
			endDateTime, _ = time.Parse("2006-01-02", test.end)

			actualStart, actualEnd := breakDateByMonth(startDateTime, endDateTime)
			if strings.Compare(test.wantStart, actualStart.Format("2006-01-02")) != 0 {
				t.Errorf("BreakDateByMonth start response does not match.\n got: %v\n want: %v", actualStart.Format("2006-01-02"), test.wantStart)
			}
			if strings.Compare(test.wantEnd, actualEnd.Format("2006-01-02")) != 0 {
				t.Errorf("BreakDateByMonth end response does not match.\n got: %v\n want: %v", actualEnd.Format("2006-01-02"), test.wantEnd)
			}
		})
	}
}

func TestGetMailmanData(t *testing.T) {
	ctx := context.Background()
	storage := newFakeStorageConnection()

	tests := []struct {
		comparisonType string
		storage        *fakeStorageConnection
		baseURL        string
		startDate      string
		endDate        string
		wantErr        error
	}{
		// Test StoreInBucket is called
		{
			comparisonType: "One month",
			storage:        storage,
			baseURL:        "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			startDate:      "1915-09-01",
			endDate:        "1915-09-30",
			wantErr:        os.ErrNotExist,
		},
		// SetDate error wrong format startDate
		{
			comparisonType: "StartDate wrong format",
			storage:        storage,
			baseURL:        "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			startDate:      "06-17",
			endDate:        "1915-09-30",
			wantErr:        fmt.Errorf("06-17"),
		},
		// SetDate error wrong format endDate
		{
			comparisonType: "EndDate wrong format",
			storage:        storage,
			baseURL:        "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			startDate:      "1915-09-01",
			endDate:        "06-17",
			wantErr:        fmt.Errorf("06-17"),
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr := GetMailmanData(ctx, test.storage, test.baseURL, test.startDate, test.endDate); !errors.Is(gotErr, test.wantErr) {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("Error doesn't match.\n got: %v\nwant it to contain: %v", gotErr, test.wantErr)
				}
			}
		})
	}
}
