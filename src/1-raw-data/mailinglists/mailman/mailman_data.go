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
	"context"
	"fmt"
	"strings"
	"time"
)

// Check dates used in the Mailman filename have value, are not the same and that start before end.
func setDates(startDate, endDate string) (time.Time, time.Time, error) {
	var startDateTime, endDateTime time.Time
	var err error

	if startDate == "" {
		startDate = time.Now().Format("2006-01-02")
	}
	if startDateTime, err = time.Parse("2006-01-02", startDate); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("Start date string conversion to DateTime threw an error: %w", err)
	}
	if endDate == "" {
		endDate = time.Now().Format("2006-01-02")
	}
	if endDateTime, err = time.Parse("2006-01-02", endDate); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("End date string conversion to DateTime threw an error: %w", err)
	}
	// TODO assess if better to throw error?
	if startDate == endDate {
		startDateTime = startDateTime.AddDate(0, 0, -1)
		startDate = startDateTime.Format("2006-01-02")
	}
	if startDateTime.After(endDateTime) {
		return startDateTime, endDateTime, fmt.Errorf("Start date %v was past end date %v. Update input with different start date.", startDate, endDate)
	}
	return startDateTime, endDateTime, nil
}

// Create filename to save Mailman data.
func createMailmanFilename(currentStart string) string {
	yearMonth := strings.Split(currentStart, "-")[0:2]
	return strings.Join(yearMonth, "-") + ".mbox.gz"
}

// Create URL needed for Mailman with specific dates and filename for output. Forces start to first of month and end to end of month unles current date.
func createMailmanURL(mailingListURL, filename, startDate, endDate string) string {
	return fmt.Sprintf("%vexport/python-dev@python.org-%v?start=%v&end=%v", mailingListURL, filename, startDate, endDate)
}

// Break dates out to span only a month, start must be 1st and end must be 1st of the following month unless today
func breakDateByMonth(startDateTime, endDateTime time.Time) (time.Time, time.Time) {
	// Change start date to the 1st of the month
	if startDateTime.Day() > 1 {
		startDateTime = startDateTime.AddDate(0, 0, -startDateTime.Day()+1)
	}

	firstDayFollowingMonth := startDateTime.AddDate(0, 1, 0)

	// End date set to first day of following month unless its today; then leave as today
	if endDateTime.Day() != 1 || firstDayFollowingMonth.Format("2006-01-02") < endDateTime.Format("2006-01-02") || firstDayFollowingMonth.Format("2006-01-02") <= time.Now().Format("2006-01-02") {
		endDateTime = firstDayFollowingMonth
	}
	return startDateTime, endDateTime
}

// Get, parse and store mailman data in GCS.
func GetMailmanData(ctx context.Context, storage gcs.GCSConnection, baseURL, startDate, endDate string) error {
	var startDateTime, endDateTime time.Time
	var filename, url string
	var err error
	if startDateTime, endDateTime, err = setDates(startDate, endDate); err != nil {
		return err
	}

	orgEndDateTime := endDateTime

	// If the date range is larger than one month, cycle and capture content by month
	for startDateTime.Format("2006-01-02") <= orgEndDateTime.Format("2006-01-02") {
		startDateTime, endDateTime = breakDateByMonth(startDateTime, endDateTime)
		filename = createMailmanFilename(startDateTime.String())
		url = createMailmanURL(baseURL, filename, startDateTime.Format("2006-01-02"), endDateTime.Format("2006-01-02"))
		if err := storage.StoreGCS(ctx, filename, url); err != nil {
			return fmt.Errorf("Storage failed: %w", err)
		}
		startDateTime = startDateTime.AddDate(0, 1, 0)
		endDateTime = endDateTime.AddDate(0, 1, 0)
	}
	return nil
}

func main() {
}
