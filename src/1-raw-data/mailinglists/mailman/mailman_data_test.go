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
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSetDates(t *testing.T) {
	// test passing in empty start, empty date, same date, start older than end, not a string
	today := time.Now().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	olderStartDateErrorExample := fmt.Errorf("Start date %v was past end date %v. Update input with different start date.", today, "1915-09-18")
	tests := []struct {
		comparisonType string
		start          string
		end            string
		wantStart      string
		wantEnd        string
		err            error
	}{
		{
			comparisonType: "Dates empty",
			wantStart:      yesterday,
			wantEnd:        today,
			err:            nil,
		},
		{
			comparisonType: "Start date empty",
			end:            "1915-09-18",
			wantStart:      yesterday,
			wantEnd:        "1915-09-18",
			err:            olderStartDateErrorExample,
		},
		{
			comparisonType: "End date empty",
			start:          "1865-06-17",
			wantStart:      "1865-06-17",
			wantEnd:        today,
			err:            nil,
		},
		{
			comparisonType: "Start and end dates provided and correct",
			start:          "1865-06-17",
			end:            "1915-09-17",
			wantStart:      "1865-06-17",
			wantEnd:        "1915-09-17",
			err:            nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotStart, gotEnd, err := setDates(test.start, test.end); err == test.err {
				if strings.Compare(gotStart, test.wantStart) != 0 {
					t.Errorf("SetDates response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotStart, test.wantStart)
				}
				if strings.Compare(gotEnd, test.wantEnd) != 0 {
					t.Errorf("SetDates response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotEnd, test.wantEnd)
				}
			} else if err.Error() != test.err.Error() {
				t.Errorf("Expected error mismatch for %v.\n got: %v\nwant: %v", test.comparisonType, err, test.err)
			}
		})
	}
}

func TestCycleDates(t *testing.T) {
	tests := []struct {
		comparisonType string
		start          string
		end            string
		wantStart       string
		wantEnd         string
	}{
		{
			comparisonType: "One month",
			start: "1915-09-01",
			end: "1915-09-30",
			wantStart: "1915-09-01",
			wantEnd: "1915-09-30",
		},
		{
			comparisonType: "Start is not the 1st and end date more than a month away",
			start: "1865-06-17",
			end:"1915-09-18",
			wantStart: "1865-06-17",
			wantEnd: "1865-06-30",
		},
		{
			comparisonType: "Start is the 1st and end date more than a month away",
			start: "1865-07-01",
			end: "1915-09-18",
			wantStart: "1865-07-01",
			wantEnd: "1865-07-31",
		},
	}
	for _, test := range tests {
		actualStart, actualEnd, err := cycleDates(test.start, test.end)
		if err != nil {
			// TODO check what the error should be
			t.Errorf("Error returned")
		}
		if strings.Compare(test.wantStart, actualStart) != 0 {
			t.Errorf("CycleDates response does not match.\n got: %v\nexpected: %v", actualStart, test.wantStart)
		}
		if strings.Compare(test.wantEnd, actualEnd) != 0 {
			t.Errorf("CycleDates response does not match.\n got: %v\nexpected: %v", actualEnd, test.wantEnd)
		}
		fmt.Printf("%v : cycleDates result matches.", test.comparisonType)

	}
}

func TestCreateMailmanFilename(t *testing.T) {
	tests := []struct {
		date string
		want string
	}{
		{
			date: "1865-06-17",
			want: "1865-06.mbox.gz",
		},
	}
	for _, test := range tests {
		got := createMailmanFilename(test.date)
		if strings.Compare(got, test.want) != 0 {
			t.Errorf("CreateMMFileName response does not match.\n got: %v\nwant: %v", got, test.want)
		}
	}
}

func TestCreateMailManURL(t *testing.T) {
	tests := []struct {
		url       string
		filename  string
		startDate string
		endDate   string
	}{
		{
			url:       "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte",
			filename:  "susan_la_flesche_picotte.mbox.gz",
			startDate: "1865-06-17",
			endDate:   "1915-09-18",
		},
	}
	for _, test := range tests {

		want := fmt.Sprintf("%vexport/python-dev@python.org-%v?start=%v&end=%v", test.url, test.filename, test.startDate, test.endDate)
		got := createMailmanURL(test.url, test.filename, test.startDate, test.endDate)
		if strings.Compare(got, want) != 0 {
			t.Errorf("CreateMMURL response does not match.\n got: %v\nwant: %v", got, want)
		}
	}
}
