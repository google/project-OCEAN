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

/*
Access and load Mailman data.
*/

package mailman

//TODO
// Add cycle to pull data by month if the start and end are multiple months
// Run this monthly at start of new month to pull all new data

import (
	"1-raw-data/gcs"
	"fmt"
	"strings"
	"time"
)

// Check dates used in the Mailman filename have value, are not the same and that start before end.
func setDates(startDate, endDate string) (string, string, error) {
	var startDateTime, endDateTime time.Time
	var err error

	if startDate == "" {
		startDate = time.Now().Format("2006-01-02")
	}
	if endDate == "" {
		endDate = time.Now().Format("2006-01-02")
	}
	if startDateTime, err = time.Parse("2006-01-02", startDate); err != nil {
		return startDate, endDate, fmt.Errorf("Date string conversion to DateTime threw an error: %v", err)
	}
	if endDateTime, err = time.Parse("2006-01-02", endDate); err != nil {
		return startDate, endDate, fmt.Errorf("Date string conversion to DateTime threw an error: %v", err)
	}
	if startDate == endDate {
		startDateTime = startDateTime.AddDate(0, 0, -1)
		startDate = startDateTime.Format("2006-01-02")
	}
	if startDateTime.After(endDateTime) {
		return startDate, endDate, fmt.Errorf("Start date %v was past end date %v. Update input with different start date.", startDate, endDate)
	}
	return startDate, endDate, nil
}

func createMailmanFilename(currentStart string) string {
	yearMonth := strings.Split(currentStart, "-")[0:2]
	return strings.Join(yearMonth, "-") + ".mbox.gz"
}

// Create URL needed for Mailman with specific dates and filename for output.
func createMailmanURL(mailingListURL, filename, startDate, endDate string) string {
	return fmt.Sprintf("%vexport/python-dev@python.org-%v?start=%v&end=%v", mailingListURL, filename, startDate, endDate)
}

func cycleDates(start, end string) (string, string, error) {
	var startDateTime, endDateTime time.Time
	var err error
	if startDateTime, err = time.Parse("2006-01-02", start); err != nil {
		return start, end, err
	}
	if endDateTime, err = time.Parse("2006-01-02", end) ; err != nil{
		return start, end, err
	}
	if startDateTime.AddDate(0, -1, 0).Month() < endDateTime.Month() {
		return start, startDateTime.AddDate(0, -1, 0).String(), nil
	}
	return "", "", nil

}

// start is at 1
// start is not at 1
// end as at 30 or 31
// end is not at 30 or 31

//func monthInterval(y int, m time.Month) (firstDay, lastDay time.Time) {
//	firstDay = time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
//	lastDay = time.Date(y, m+1, 1, 0, 0, 0, -1, time.UTC)
//	return firstDay, lastDay
//}


// Get, parse and store mailman data in GCS.
func GetMailmanData(gcs gcs.StorageConnection, mailingListURL, startDate, endDate string) error {
	// TODO cycle through dates if they are more than a month apart
	if start, end, err := setDates(startDate, endDate); err != nil {
		return err
	} else {
		fmt.Println(start, end)
	}

	//if convertDateTime(endDate).Add(-convertDateTime(startDate)) > 30
	filename := createMailmanFilename(startDate)
	url := createMailmanURL(mailingListURL, filename, startDate, endDate)

	if err := gcs.StoreGCS(filename, url); err != nil {
		return fmt.Errorf("Storage failed: %v", err)
	}
	return nil
}

func main() {
}
