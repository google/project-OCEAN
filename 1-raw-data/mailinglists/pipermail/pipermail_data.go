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
Access and load Pipermail data.
*/

package pipermail

//TODO
// Add test of the specific page format expected and how to parse it

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/utils"
)

var (
	StorageErr = errors.New("Storage failed")
)

func changeMonthToDigit(fileName string) (newName string, fileDate time.Time) {
	fileNameParts := strings.SplitN(fileName, ".", 2)
	fileNameDateParts := strings.Split(fileNameParts[0], "-")
	year, month := fileNameDateParts[0], fileNameDateParts[1]
	fileDate, _ = time.Parse("2006-January-02", fmt.Sprintf("%s-%s-01", year, month))
	newName = fmt.Sprintf("%s-%02d.%s", year, int(fileDate.Month()), fileNameParts[1])
	return
}

// Get, parse and store Pipermail data in GCS.
func GetPipermailData(ctx context.Context, storage gcs.Connection, groupName, startDateString, endDateString string, httpToDom utils.HttpDomResponse) (storeErr error) {
	mailingListURL := fmt.Sprintf("https://mail.python.org/pipermail/%s/", groupName)
	log.Printf("PIPERMAIL loading")

	var (
		dom                        *goquery.Document
		err                        error
		startDateTime, endDateTime time.Time
	)

	if dom, err = httpToDom(mailingListURL); err != nil {
		return fmt.Errorf("HTTP dom error: %v", err)
	}
	if startDateTime, err = utils.GetDateTimeType(startDateString); err != nil {
		return fmt.Errorf("start date: %v", err)
	}
	if endDateTime, err = utils.GetDateTimeType(endDateString); err != nil {
		return fmt.Errorf("end date: %v", err)
	}

	dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
		filename, ok := s.Attr("href")
		if ok {
			check := strings.Split(filename, ".")
			len := len(check) - 1
			if check[len] == "gz" {
				if strings.Split(filename, ":")[0] != "https" {
					url := fmt.Sprintf("%v%v", mailingListURL, filename)
					revisedFileName, fileDate := changeMonthToDigit(filename)
					if utils.InTimeSpan(fileDate, startDateTime, endDateTime) {
						if _, err = storage.StoreContentInBucket(ctx, revisedFileName, url, "url"); err != nil {
							// Each func interface doesn't allow passing errors?
							storeErr = fmt.Errorf("%w: %v", StorageErr, err)
						}
					}
				}
			}
		}
	})
	return storeErr
}

func main() {
}
