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
	dateSetErr = fmt.Errorf("set date")
	dateParseErr = fmt.Errorf("parse date")
	splitMonthErr = fmt.Errorf("split month")
)

// Func pointer to create HTTP response body and return as a string
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

func GetDateTimeType(dateString string)(dateTime time.Time, err error) {
	if dateTime, err = time.Parse("2006-01-02", dateString); err != nil {
		err = fmt.Errorf("%w: %v", dateParseErr, err)
	}
	return
}

//Define and return start and end date strings for mailing list data load
func FixEmptyDate(currentDate time.Time, startDate, endDate string) (startDateResult, endDateResult string, err error) {
	var startDateTime, endDateTime time.Time

	//If empty start date, make it the current date minus 1 day so it doesn't equal end date
	if startDate == "" {
		startDateTime = currentDate.AddDate(0, 0, -1)
		startDateResult = startDateTime.Format("2006-01-02")
		log.Printf("Start date not set so it was set to 1 day before the current date")
	} else {
		//Get start date as time type to compare to end date
		if startDateTime, err = time.Parse("2006-01-02", startDate); err != nil {
			err = fmt.Errorf("%w: start date: %v", dateParseErr, err)
		}
	}

	//If empty end date or its a time before now reset to current date
	if endDate == "" || endDate > currentDate.Format("2006-01-02") {
		endDateTime = currentDate
		endDateResult = endDateTime.Format("2006-01-02")
		log.Printf("Start date not set so it was set to 1 day before the current date")
	} else {
		//Get end date as time type to compare to end date
		if endDateTime, err = time.Parse("2006-01-02", endDate); err != nil {
			err = fmt.Errorf("%w: end date: %v", dateParseErr, err)
		}
	}

	if startDateTime.After(endDateTime) {
		err = fmt.Errorf("%w start date %v was past end date %v. Update input with different start date.", dateSetErr, startDate, endDate)
	}

	// Return start dates and end date strings passed in that aren't empty and start is before end
	return
}

// Create month span dates so start must be 1st and end must be 1st of the following month unless today
func SplitDatesByMonth(currentDate time.Time, startDate, endDate string, numMonths int) (startDaeResult, endDateResult string, err error) {
	if startDate = ""  ||  endDate = "" {
		err = fmt.Errorf("%w make sure start date %v and end date %v are not empty and resubmit.", splitMonthErr, startDate, endDate)
		return

	}

	// Change start date to the 1st of the month
	if startDateTime.Day() > 1 {
		startDateTime = startDateTime.AddDate(0, 0, -startDateTime.Day()+1)
	}

	startDateMinusNumMonths := startDateTime.AddDate(0, -numMonths, 0)
	startDaeResult = startDateMinusNumMonths.Format("2006-01-02")

	// End date set to first day of following month unless its today; then leave as today
	if endDateTime.Day() != 1 || startDateMinusNumMonths.Format("2006-01-02") < endDateTime.Format("2006-01-02") || firstDayFollowingMonth.Format("2006-01-02") <= currentDate.Format("2006-01-02") {
		endDateTime = firstDayFollowingMonth
	}
	return
}