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

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCreateMailManURL(t *testing.T) {
	*mailingListURL = "https://en.wikipedia.org/wiki/Susan_La_Flesche_Picotte"
	filename := "susan_la_flesche_picotte.mbox.gz"
	startDate := "1865-06-17"
	endDate := "1915-09-18"
	expected := *mailingListURL + "export/python-dev@python.org-" + filename + "?start=" + startDate + "&end=" + endDate

	actual := createMMURL(filename, startDate, endDate)

	if strings.Compare(expected, actual) != 0 {
		t.Errorf("CreateMMURL response does not match.\n got: %v\nexpected: %v", actual, expected)
	}
	fmt.Println("CreateMMURL results match")
}

func TestConvertDateTime(t *testing.T) {
	date := "1865-06-17"
	expected, _ := time.Parse("1865-06-17", date)
	actual := convert2DateTime(date)

	if expected == actual {
		t.Errorf("ConvertDateTime response does not match.\n got: %v\nexpected: %v", actual, expected)
	}
	fmt.Println("ConvertDateTime results match")
}

func TestCreateMMFileName(t *testing.T) {
	date := "1865-06-17"
	expected := "1865-06.mbox.gz"
	actual := createMMFileName(date)
	if strings.Compare(expected, actual) != 0 {
		t.Errorf("CreateMMFileName response does not match.\n got: %v\nexpected: %v", actual, expected)
	}
	fmt.Println("CreateMMFileName results match")

}

func TestSetDates(t *testing.T) {
	// test passing in empty start, empty date, same date, start older than end, not a string
	today := time.Now().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	tests := []struct {
		comparisonType string
		start          string
		end            string
		expStart       string
		expEnd         string
	}{
		{"Dates empty", "", "", yesterday, today},
		{"Start date empty", "", "1915-09-18", "1915-09-17", "1915-09-18"},
		{"End date empty", "1865-06-17", "", "1865-06-17", today},
		{"Start and end dates provided and correct", "1865-06-17", "1915-09-17", "1865-06-17", "1915-09-17"},
	}

	for _, test := range tests {
		actualStart, actualEnd := setDates(test.start, test.end)
		if strings.Compare(test.expStart, actualStart) != 0 {
			t.Errorf("SetDates response does not match.\n got: %v\nexpected: %v", actualStart, test.expStart)
		}
		if strings.Compare(test.expEnd, actualEnd) != 0 {
			t.Errorf("SetDates response does not match.\n got: %v\nexpected: %v", actualEnd, test.expEnd)
		}
		fmt.Printf("%v : setDates result matches.", test.comparisonType)
	}

}
