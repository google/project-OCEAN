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

func TestCreateFileName(t *testing.T) {
	tests := []struct {
		comparisonType string
		mailingList    string
		groupName      string
		date           string
		wantName       string
		wantErr        error
	}{
		{
			comparisonType: "Test googlegroups name created.",
			mailingList:    "gg",
			groupName:      "lead",
			date:           "1888-07-31",
			wantName:       "gg-lead/1888-07-gg-lead.txt",
			wantErr:        nil,
		},
		{
			comparisonType: "Test mailman name created.",
			mailingList:    "mailmain",
			groupName:      "LaDuke",
			date:           "2021-01-02",
			wantName:       "mailmain-LaDuke/2021-02-mailmain-LaDuke.mbox.gz",
			wantErr:        nil,
		},
		{
			comparisonType: "Test pipermail name created.",
			mailingList:    "pipermail",
			groupName:      "environmentalist",
			date:           "1989-08-07",
			wantName:       "pipermail-environmentalist/1989-08-pipermail-environmentalist.txt.gz",
			wantErr:        nil,
		},
	}

	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotName, gotErr := CreateFileName(test.mailingList, test.groupName, test.date); !errors.Is(gotErr, test.wantErr) {
				if !strings.Contains(gotErr.Error(), test.wantErr.Error()) {
					t.Errorf("CreateFileName response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
				}
				if strings.Compare(test.wantName, gotName) != 0 {
					t.Errorf("Failed creating filename. Got: %v and wanted: %v.", gotName, test.wantName)
				}
			}
		})
	}
}

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

func TestAddMonth(t *testing.T) {
	tests := []struct {
		comparisonType string
		date           time.Time
		wantTime       time.Time
	}{
		{
			comparisonType: "Test add month works.",
			date:           time.Date(1989, 8, 1, 0, 0, 0, 0, time.UTC),
			wantTime:       time.Date(1989, 9, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			comparisonType: "Test added month to leap year 2001.",
			date:           time.Date(2000, 2, 1, 0, 0, 0, 0, time.UTC),
			wantTime:       time.Date(2000, 3, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			comparisonType: "Test added month to non leap year 2007.",
			date:           time.Date(2007, 2, 1, 0, 0, 0, 0, time.UTC),
			wantTime:       time.Date(2007, 3, 3, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			gotTime := AddMonth(test.date)
			if !test.wantTime.Equal(gotTime) {
				t.Errorf("Failed add 1 month to date. Got: %v and wanted: %v.", gotTime, test.wantTime)
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
			start:          "2020-10-01",
			end:            "3020-10-01",
			numMonths:      1,
			wantStart:      ChangeFirstMonth(currentDate.AddDate(0, -1, 0)).Format("2006-01-02"),
			wantEnd:        ChangeFirstMonth(currentDate).Format("2006-01-02"),
			err:            nil,
		},
		{
			comparisonType: "End date after today",
			start:          "2020-12-01",
			end:            "3020-12-30",
			numMonths:      1,
			wantStart:      ChangeFirstMonth(currentDate.AddDate(0, -1, 0)).Format("2006-01-02"),
			wantEnd:        ChangeFirstMonth(currentDate).Format("2006-01-02"),
			err:            nil,
		},
		{
			comparisonType: "Check Feb\n",
			start:          "2009-01-01",
			end:            "2009-01-31",
			numMonths:      1,
			wantStart:      "2009-01-01",
			wantEnd:        "2009-02-01",
			err:            nil,
		},
		{
			comparisonType: "Check multi months\n",
			start:          "2020-02-01",
			end:            "2020-04-18",
			numMonths:      3,
			wantStart:      "2020-02-01",
			wantEnd:        "2020-05-01",
			err:            nil,
		},
		{
			comparisonType: "End month earlier than start",
			start:          "2020-12-01",
			end:            "2021-01-01",
			numMonths:      2,
			wantStart:      "2020-11-01",
			wantEnd:        "2021-01-01",
			err:            nil,
		},
		{
			comparisonType: "Split by more than a year",
			start:          "2020-12-01",
			end:            "2021-01-01",
			numMonths:      23,
			wantStart:      "2019-02-01",
			wantEnd:        "2021-01-01",
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
