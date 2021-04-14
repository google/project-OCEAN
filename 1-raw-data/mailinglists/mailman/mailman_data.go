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

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/utils"
)

var (
	storageErr = errors.New("Storage failed")
)

// Create filename to save Mailman data.
func createMailmanFilename(currentStart string) (fileName string) {
	yearMonth := strings.Split(currentStart, "-")[0:2]
	return strings.Join(yearMonth, "-") + ".mbox.gz"
}

// Create URL needed for Mailman with specific dates and filename for output. Forces start to first of month and end to end of month unless current date.
func createMailmanURL(mailingListURL, filename, startDate, endDate string) (url string) {
	return fmt.Sprintf("%vexport/python-dev@python.org-%v?start=%v&end=%v", mailingListURL, filename, startDate, endDate)
}

// Get, parse and store mailman data in GCS.
func GetMailmanData(ctx context.Context, storage gcs.Connection, groupName, startDate, endDate string, numMonths int) (err error) {
	var startDateResult, endDateResult string
	var startDateTime, endDateTime time.Time
	var filename, url string
	mailingListURL := fmt.Sprintf("https://mail.python.org/archives/list/%s@python.org/", groupName)
	log.Printf("MAILMAN loading")

	// Check dates have value, are not the same and that start before end.
	if startDateResult, endDateResult, err = utils.FixDate(startDate, endDate); err != nil {
		return
	}

	orgEndDate := endDateResult

	// Cycle and capture content by month
	for startDateResult < orgEndDate {
		// Break dates by month duration
		if startDateResult, endDateResult, err = utils.SplitDatesByMonth(startDateResult, endDateResult, numMonths); err != nil {
			return
		}

		filename = createMailmanFilename(startDateResult)

		url = createMailmanURL(mailingListURL, filename, startDateResult, endDateResult)
		if _, err = storage.StoreContentInBucket(ctx, filename, url, "url"); err != nil {
			return fmt.Errorf("%w: %v", storageErr, err)
		}

		// Update the dates for the loop to continue if endDate is less
		startDateTime, _ = utils.GetDateTimeType(startDateResult)
		startDateResult = utils.ChangeFirstMonth(utils.AddMonth(startDateTime)).Format("2006-01-02")
		if endDateResult < orgEndDate {
			endDateTime, _ = utils.GetDateTimeType(endDateResult)
			endDateResult = utils.ChangeFirstMonth(utils.AddMonth(endDateTime)).Format("2006-01-02")
		}
	}

	if endDateResult < orgEndDate {
		log.Printf("Did not copy all dates. Stopped at %v vs. orginal date: %v", endDateResult, orgEndDate)
		return fmt.Errorf("%w to get all the dates, stopped at: %v when expected to stop at: %v", storageErr, endDateResult, orgEndDate)
	}
	return
}

func main() {
}
