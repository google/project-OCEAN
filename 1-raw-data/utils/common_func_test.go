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

package utils

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFixDates(t *testing.T) {
	// Test passing in empty start, empty date, same date, start older than end, not a string
	currentDate := time.Now()
	today := currentDate.Format("2006-01-02")
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
			err:            dateFixErr,
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
		{
			comparisonType: "Parse error on start date",
			start:          "06-17",
			end:            "1915-09-18",
			wantStart:      "0001-01-01",
			wantEnd:        "1915-09-18",
			err:            nil,
		},
		{
			comparisonType: "Parse error on end date",
			start:          "1865-06-17",
			end:            "09-18",
			wantStart:      "1865-06-17",
			wantEnd:        "0001-01-01",
			err:            dateFixErr,
		},
		{
			comparisonType: "Parse error start date is after end date",
			start:          "1965-06-17",
			end:            "1865-09-18",
			wantStart:      "1965-06-17",
			wantEnd:        "1865-09-18",
			err:            dateFixErr,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotStart, gotEnd, gotErr := FixDate(test.start, test.end); errors.Is(gotErr, test.err) {
				if strings.Compare(gotStart, test.wantStart) != 0 {
					t.Errorf("SetDates start response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotStart, test.wantStart)
				}
				if strings.Compare(gotEnd, test.wantEnd) != 0 {
					t.Errorf("SetDates end response does not match for %v.\n got: %v\nwant: %v", test.comparisonType, gotEnd, test.wantEnd)
				}
			} else if !strings.Contains(gotErr.Error(), test.err.Error()) {
				t.Errorf("Expected error mismatch for %v.\n got: %v\nwant: %v", test.comparisonType, gotErr, test.err)
			}
		})
	}
}

func TestSplitDatesByMonth(t *testing.T) {
	currentDate := time.Now()

	tests := []struct {
		comparisonType string
		start          string
		end            string
		numMonths      int
		wantStart      string
		wantEnd        string
		err            error
	}{
		{
			comparisonType: "One month.\n",
			start:          "1915-09-01",
			end:            "1915-09-30",
			numMonths:      1,
			wantStart:      "1915-09-01",
			wantEnd:        "1915-10-01",
			err:            nil,
		},
		{
			comparisonType: "Start not 1st and end over a month after\n",
			start:          "1865-01-17",
			end:            "1865-07-01",
			numMonths:      1,
			wantStart:      "1865-06-01",
			wantEnd:        "1865-07-01",
			err:            nil,
		},
		{
			comparisonType: "Start is 1st and end over a month after\n",
			start:          "1865-06-01",
			end:            "1865-08-01",
			numMonths:      1,
			wantStart:      "1865-07-01",
			wantEnd:        "1865-08-01",
			err:            nil,
		},
		{
			comparisonType: "End is not the 1st of the following month\n",
			start:          "1865-07-01",
			end:            "1865-07-18",
			numMonths:      1,
			wantStart:      "1865-07-01",
			wantEnd:        "1865-08-01",
			err:            nil,
		},
		{
			comparisonType: "End date after today if start of month",
			start:          "2020-09-01",
			end:            "3020-09-01",
			numMonths:      1,
			wantStart:      currentDate.AddDate(0, -1, 0).Format("2006-01-02"),
			wantEnd:        currentDate.Format("2006-01-02"),
			err:            nil,
		},
		{
			comparisonType: "End date after today",
			start:          "2020-09-01",
			end:            "3020-09-30",
			numMonths:      1,
			wantStart:      currentDate.AddDate(0, -1, 0).Format("2006-01-02"),
			wantEnd:        currentDate.Format("2006-01-02"),
			err:            nil,
		},
		{
			comparisonType: "Check Feb\n",
			start:          "1865-02-01",
			end:            "1865-03-18",
			numMonths:      1,
			wantStart:      "1865-03-01",
			wantEnd:        "1865-04-01",
			err:            nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {

			if startDate, endDate, gotErr := SplitDatesByMonth(test.start, test.end, test.numMonths); errors.Is(gotErr, test.err) {
				if strings.Compare(test.wantStart, startDate) != 0 {
					t.Errorf("BreakDateByMonth start response does not match.\n got: %v\n want: %v", startDate, test.wantStart)
				}
				if strings.Compare(test.wantEnd, endDate) != 0 {
					t.Errorf("BreakDateByMonth end response does not match.\n got: %v\n want: %v", endDate, test.wantEnd)
				}
			}
		})
	}
}