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
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var (
	httpStrRespErr = fmt.Errorf("http string")
	httpDomRespErr = fmt.Errorf("http dom")
	dateFixErr     = fmt.Errorf("fix date")
	dateParseErr   = fmt.Errorf("parse date")
	splitMonthErr  = fmt.Errorf("split month")
)

//TODO - retry load if it fails
//Func pointer to create HTTP response body and return as a string
type HttpStringResponse func(string) (string, error)

// Create HTTP response body and return as a string
func StringResponse(url string) (responseString string, err error) {
	var (
		bodyBytes []byte
		response  *http.Response
	)

	// Keep program running even when url is empty. Returns emptry string and nil error
	if url == "" {
		return
	}

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("%w response returned an error: %v", httpStrRespErr, err)
		return
	}
	defer response.Body.Close()

	if bodyBytes, err = ioutil.ReadAll(response.Body); err != nil {
		//if errors.Is(err, syscall.EPIPE) {
		//	log.Printf("HTTP string get broken pipe ignored for url: %s/n", url)
		//} else {
		err = fmt.Errorf("%w reading bodybytes failed: %v", httpStrRespErr, err)
		return
	}

	responseString = string(bodyBytes)
	return
}

// Func pointer to create HTTP response body and return as a dom object
type HttpDomResponse func(string) (*goquery.Document, error)

// Create HTTP response body and return as a dom object
func DomResponse(url string) (dom *goquery.Document, err error) {
	var response *http.Response

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("%w returned an error: %v", httpDomRespErr, err)
		return
	}
	defer response.Body.Close()

	if dom, err = goquery.NewDocumentFromReader(response.Body); err != nil {
		err = fmt.Errorf("%w goquery dom conversion returned an error: %v", httpDomRespErr, err)
		return
	}
	return
}

//Add subdirectory and date to filename
func CreateFileName(mailingList, groupName, date string) (newFileName string, err error) {
	var (
		subDirFinal, fileType string
		fileDate              time.Time
	)

	switch mailingList {
	case "gg":
		fileType = "txt"
	case "mailman":
		fileType = "mbox.gz"
	case "pipermail":
		fileType = "txt.gz"
	}

	subDirFinal = fmt.Sprintf("%s-%s", mailingList, groupName)

	if fileDate, err = GetDateTimeType(date); err != nil {
		err = fmt.Errorf("start date: %v", err)
	}

	newFileName = fmt.Sprintf("%s/%04d-%02d-%s.%s", subDirFinal, fileDate.Year(), int(fileDate.Month()), subDirFinal, fileType)

	log.Printf("Final filename %s ", newFileName)
	return
}

// Convert date string to time type
func GetDateTimeType(dateString string) (dateTime time.Time, err error) {
	if dateTime, err = time.Parse("2006-01-02", dateString); err != nil {
		err = fmt.Errorf("%w: %v", dateParseErr, err)
	}
	return
}

// Verify a date is in between a timespan
func InTimeSpan(fileDate, startDateTime, endDateTime time.Time) (load bool) {
	return (fileDate.After(startDateTime) || fileDate == startDateTime) && (fileDate.Before(endDateTime) || fileDate == endDateTime)
}

//Define and return start and end date strings for mailing list data load
func FixDate(startDate, endDate string) (startDateResult, endDateResult string, err error) {
	var startDateTime, endDateTime time.Time
	currentDate := time.Now()

	//log.Printf("START & END DATES", startDate, endDate)
	//If empty start date, make it the current date minus 1 day so it doesn't equal end date
	if startDate == "" {
		startDateTime = currentDate.AddDate(0, 0, -1)
		startDateResult = startDateTime.Format("2006-01-02")
		log.Printf("Start date empty. It was set to 1 day before the current date.")
	} else {
		//Get start date as time type to compare to end date
		if startDateTime, err = GetDateTimeType(startDate); err != nil {
			err = fmt.Errorf("start date: %v", err)
		}
		// Reformat incase malformed date string provided
		startDateResult = startDateTime.Format("2006-01-02")
	}

	//If empty end date or its a time before now reset to current date
	if endDate == "" || endDate > currentDate.Format("2006-01-02") {
		endDateTime = currentDate
		endDateResult = endDateTime.Format("2006-01-02")
		log.Printf("End date empty. It was set to current date")
	} else {
		//Get end date as time type to compare to end date
		if endDateTime, err = GetDateTimeType(endDate); err != nil {
			err = fmt.Errorf("end date: %v", err)
		}
		// Reformat incase malformed date string provided
		endDateResult = endDateTime.Format("2006-01-02")
	}

	if startDateResult == endDateResult {
		if startDateResult, endDateResult, err = SplitDatesByMonth(startDateResult, endDateResult, 1); err != nil {
			err = fmt.Errorf("%w start date %v and end date %v are not able to be fixed to 1 month split.", dateFixErr, startDate, endDate)
		}
		log.Printf("startDate, %s, changed to 1 month before endDate, %s because they were equal. Change startDate if you want something different.", startDateResult, endDateResult)
	}

	if startDateTime.After(endDateTime) {
		err = fmt.Errorf("%w start date %v was past end date %v. Update input with different start date.", dateFixErr, startDate, endDate)
	}
	// Return start dates and end date strings passed in that aren't empty and start is before end
	return
}

// Change date to the 1st of the month
func ChangeFirstMonth(dateTime time.Time) (dateTimeResult time.Time) {
	if dateTime.Day() > 1 {
		dateTimeResult = dateTime.AddDate(0, 0, -dateTime.Day()+1)
		//dateTimeResult = time.Date(dateTime.Year(), dateTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	} else {
		dateTimeResult = dateTime
	}
	return
}

//Add month workaround for AddDate normalizing that causes behavior like adding a month to end of Jan to point to March.
func AddMonth(date time.Time) (dateResult time.Time) {
	// Add a month which pushes to the start of a month after what you want
	month := int(date.Month())
	dateTemp := ChangeFirstMonth(date)
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		dateResult = dateTemp.AddDate(0, 0, 31)
	case 2:
		dateResult = dateTemp.AddDate(0, 0, 30)
		dateResult = time.Date(dateResult.Year(), dateResult.Month(), 0, 0, 0, 0, 0, time.UTC)
	default:
		dateResult = dateTemp.AddDate(0, 1, 0)
	}
	if date.Day() > 1 {
		dateResult.AddDate(0, 0, date.Day()-1)
	}
	return
}

// Create month span dates so start must be 1st and end must be 1st of the following month unless today.
func SplitDatesByMonth(startDate, endDate string, numMonths int) (startDateResult, endDateResult string, err error) {
	var startDateTime, endDateTime time.Time
	currentDate := time.Now()

	if startDate == "" || endDate == "" {
		if startDateResult, endDateResult, err = FixDate(startDate, endDate); err != nil {
			err = fmt.Errorf("%w make sure start date %v and end date %v are not empty and resubmit.", splitMonthErr, startDate, endDate)
		}
	}

	//Get start date as time type
	if startDateTime, err = GetDateTimeType(startDate); err != nil {
		err = fmt.Errorf("start date: %v", err)
	}

	// Change start date to the 1st of the month
	startDateTime = ChangeFirstMonth(startDateTime)

	//Get end date as time type
	if endDateTime, err = GetDateTimeType(endDate); err != nil {
		err = fmt.Errorf("end date: %v", err)
	}

	if endDateTime.After(currentDate) {
		//Set end date to current date if end date is after current date
		endDateTime = ChangeFirstMonth(currentDate)
	} else if endDateTime.Day() > 1 && currentDate.After(endDateTime) {
		//If end date past the first of the month and not in current month, set to the start of next month.
		log.Printf("End date after 1st of month and not the current date so it is moved to the following month.")
		endDateTime = ChangeFirstMonth(AddMonth(endDateTime))
	}

	// Check that start and end are separated by number of months
	if int(endDateTime.Month()-startDateTime.Month()) != numMonths {
		startDateTime = endDateTime.AddDate(0, -numMonths, 0)
		log.Printf("startDate, %s, changed to num months before endDate, %s. Change endDate if you want something different.", startDateTime.String(), endDateTime.String())
	}

	startDateResult = startDateTime.Format("2006-01-02")
	endDateResult = endDateTime.Format("2006-01-02")
	return
}
