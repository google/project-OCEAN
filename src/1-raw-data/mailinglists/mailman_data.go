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

//TODO
// Add cycle to pull data by month if the start and end are multiple months
// Run this monthly at start of new month to pull all new data

import (
	"log"
	"strings"
	"time"
)

func createMMURL(filename, startDate, endDate string) string {
	url := *mailingListURL + "export/python-dev@python.org-" + filename + "?start=" + startDate + "&end=" + endDate
	return url
}

func convert2DateTime(date string) time.Time {
	myDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return myDate
}

func createMMFileName(currentStart string) string {
	yr_mt := strings.Split(currentStart, "-")[0:2]
	return strings.Join(yr_mt, "-") + ".mbox.gz"
}

func setDates(startDate, endDate string) (string, string) {
	if startDate == "" {
		startDate = time.Now().Format("2006-01-02")
	}
	if endDate == "" {
		endDate = time.Now().Format("2006-01-02")
	}
	if startDate == endDate {
		startDateT := convert2DateTime(startDate)
		startDate = startDateT.AddDate(0, 0, -1).Format("2006-01-02")
	}
	if convert2DateTime(startDate).After(convert2DateTime(endDate)) {
		orgStartDate := startDate
		startDate = convert2DateTime(endDate).AddDate(0, 0, -1).Format("2006-01-02")
		log.Printf("Start date %v was past end date %v. It was changed to %v which is 1 day less than end date. Update input if a different start date is expected.\n", orgStartDate, startDate, endDate)
	}
	return startDate, endDate
}

func cycleDates(start, end time.Time) (string, string) {
	if start.Month()
	if start.AddDate(0, 0, -30){}
	return "", ""
}

//func monthInterval(y int, m time.Month) (firstDay, lastDay time.Time) {
//	firstDay = time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
//	lastDay = time.Date(y, m+1, 1, 0, 0, 0, -1, time.UTC)
//	return firstDay, lastDay
//}

func mailManMain() {
	// TODO cycle through dates if they are more than a month apart
	setDates(*startDate, *endDate)
	//if convertDateTime(endDate).Add(-convertDateTime(startDate)) > 30

	filename := createMMFileName(*startDate)
	url := createMMURL(filename, *startDate, *endDate)
	gcs.storeGCS(filename, url)
}
