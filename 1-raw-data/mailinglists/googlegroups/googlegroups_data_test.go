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

package googlegroups

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/utils"
)

func TestHttpStringResponse(t *testing.T) {
	var gotResponseString string
	var gotErr error

	tests := []struct {
		comparisonType     string
		url                string
		wantResponseString string
		wantErr            error
	}{
		{
			comparisonType:     "Get empty response if no url",
			url:                "",
			wantResponseString: "",
			wantErr:            nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {

			if gotResponseString, gotErr = utils.StringResponse(test.url); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if strings.Compare(gotResponseString, test.wantResponseString) != 0 {
				t.Errorf("Filename response does not match.\n got: %v\nwant: %v", gotResponseString, test.wantResponseString)
			}
		})
	}
}

// TODO test for error?
func TestGetFileDate(t *testing.T) {
	var (
		gotDate time.Time
		gotErr  error
	)

	wantDateKeyOneTwo, _ := utils.GetDateTimeType("1991-01-29")
	wantDateKeyOneOne, _ := utils.GetDateTimeType("1938-09-02")
	wantDateKeyTwoOne, _ := utils.GetDateTimeType("1995-10-02")
	wantDateKeyTwoTwo, _ := utils.GetDateTimeType("2017-11-11")

	tests := []struct {
		comparisonType string
		matchDate      string
		wantFileDate   time.Time
		wantErr        error
	}{
		{
			// Confirm correct output with 1 dig month and 2 dig day
			comparisonType: "Single digit month and double digit day",
			matchDate:      "1/29/91",
			wantFileDate:   wantDateKeyOneTwo,
			wantErr:        nil,
		},
		{
			// Confirm correct output with 1 dig month and 1 dig day and not future
			comparisonType: "Single digit month and single digit day",
			matchDate:      "9/2/38",
			wantFileDate:   wantDateKeyOneOne,
			wantErr:        nil,
		},
		{
			// Confirm correct output with 2 dig month and 1 dig day
			comparisonType: "Double digit month and single digit day",
			matchDate:      "10/2/95",
			wantFileDate:   wantDateKeyTwoOne,
			wantErr:        nil,
		},
		{
			// Confirm correct output with 2 dig month and 2 dig day
			comparisonType: "Double digit month and double digit day",
			matchDate:      "11/11/17",
			wantFileDate:   wantDateKeyTwoTwo,
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotDate, gotErr = getFileDate(test.matchDate); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if gotDate != test.wantFileDate {
				t.Errorf("Filedate response does not match.\n got: %v\nwant: %v", gotDate, test.wantFileDate)
			}
		})
	}
}

func TestGetFileName(t *testing.T) {
	var gotDateKey string
	matchDateOneTwo, _ := utils.GetDateTimeType("1991-01-01")
	matchDateOneOne, _ := utils.GetDateTimeType("1938-09-02")
	matchDateTwoOne, _ := utils.GetDateTimeType("1995-10-02")
	matchDateTwoTwo, _ := utils.GetDateTimeType("2017-11-11")

	tests := []struct {
		comparisonType string
		matchDate      time.Time
		wantFileName   string
	}{
		{
			// Confirm correct output with 1 dig month and 2 dig day
			comparisonType: "Single digit month and double digit day",
			matchDate:      matchDateOneTwo,
			wantFileName:   "1991-01.txt",
		},
		{
			// Confirm correct output with 1 dig month and 1 dig day and not future
			comparisonType: "Single digit month and single digit day",
			matchDate:      matchDateOneOne,
			wantFileName:   "1938-09.txt",
		},
		{
			// Confirm correct output with 2 dig month and 1 dig day
			comparisonType: "Single digit month and double digit day",
			matchDate:      matchDateTwoOne,
			wantFileName:   "1995-10.txt",
		},
		{
			// Confirm correct output with 2 dig month and 2 dig day
			comparisonType: "Double digit month and double digit day",
			matchDate:      matchDateTwoTwo,
			wantFileName:   "2017-11.txt",
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			gotDateKey = getFileName(test.matchDate)
			if strings.Compare(gotDateKey, test.wantFileName) != 0 {
				t.Errorf("Filename response does not match.\n got: %v\nwant: %v", gotDateKey, test.wantFileName)
			}
		})
	}
}

func TestGetTotalTopics(t *testing.T) {
	exCorrectDom100, _ := utils.FakeHttpDomResponse("https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopics100")
	exCorrectDomLess, _ := utils.FakeHttpDomResponse("https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopicsLess")
	exMissingDom, _ := utils.FakeHttpDomResponse("https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopicsMissBody")

	var (
		gotTotalTopics int
	)

	tests := []struct {
		comparisonType  string
		dom             *goquery.Document
		wantTotalTopics int
	}{
		{
			comparisonType:  "Test regex to read total topics more than 100.",
			dom:             exCorrectDom100,
			wantTotalTopics: 100,
		},
		{
			comparisonType:  "Test regex to read total topics less than 100.",
			dom:             exCorrectDomLess,
			wantTotalTopics: 1,
		},
		{
			comparisonType:  "Test regex if info does not exist.",
			dom:             exMissingDom,
			wantTotalTopics: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			gotTotalTopics = getTotalTopics(test.dom)
			if gotTotalTopics != test.wantTotalTopics {
				t.Errorf("Total topic response does not match.\n got: %v\nwant: %v", gotTotalTopics, test.wantTotalTopics)
			}
		})
	}
}

func TestGetMsgIDsFromDom(t *testing.T) {

	exMsgIdDom, _ := utils.FakeHttpDomResponse("msgIdsFromDom")

	var (
		gotRawMsgUrl string
	)
	tests := []struct {
		comparisonType string
		org            string
		topicId        string
		group          string
		dom            *goquery.Document
		wantRawMsgUrl  string
	}{
		{
			comparisonType: "Output raw msg url",
			topicId:        "Kamakaʻeha",
			group:          "queen",
			dom:            exMsgIdDom,
			wantRawMsgUrl:  "https://groups.google.com/forum/message/raw?msg=queen/Kamakaʻeha/1891",
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			gotRawMsgUrl = getMsgIDsFromDom(test.org, test.topicId, test.group, test.dom)
			if strings.Compare(gotRawMsgUrl, test.wantRawMsgUrl) != 0 {
				t.Errorf("DateKey response does not match.\n got: %v\nwant: %v", gotRawMsgUrl, test.wantRawMsgUrl)
			}
		})
	}
}

func TestTopicIDToRawMsgUrlMap(t *testing.T) {
	now := time.Now()
	timeMonthCheck, timeYearCheck := now.Month(), now.Year()

	exTopicIdDomTime, _ := utils.FakeHttpDomResponse("topicIDToRawMsgUrlMapTime")
	exTopicIdDomDate, _ := utils.FakeHttpDomResponse("topicIDToRawMsgUrlMapDate")
	exAbuseHiddenMsg, _ := utils.FakeHttpDomResponse("abuseHiddenMsg")
	startDateTimeIdsTime, _ := utils.GetDateTimeType(now.Format("2006-01-02"))
	endDateTimeIdsTime, _ := utils.GetDateTimeType(now.Format("2006-01-02"))
	startDateTimeIdsDate, _ := utils.GetDateTimeType("2018-09-01")
	endDateTimeIdsDate, _ := utils.GetDateTimeType("2018-09-30")

	var (
		gotRawMsgURLMap map[string][]string
		gotErr          error
	)
	tests := []struct {
		comparisonType   string
		org              string
		groupName        string
		startDateTime    time.Time
		endDateTime      time.Time
		dom              *goquery.Document
		wantRawMsgURLMap map[string][]string
		wantErr          error
	}{
		{
			comparisonType: "Pull topic ids for time",
			org:            "",
			groupName:      "golang-checkins",
			startDateTime:  startDateTimeIdsTime,
			endDateTime:    endDateTimeIdsTime,
			dom:            exTopicIdDomTime,
			wantRawMsgURLMap: map[string][]string{
				fmt.Sprintf("%4d-%02d.txt", timeYearCheck, timeMonthCheck): []string{"https://groups.google.com/forum/message/raw?msg=golang-checkins/8sv65_WCOS4/3Fc-diD_AwAJ"}},
			wantErr: nil,
		},
		{
			comparisonType: "Pull topic ids for date",
			org:            "",
			groupName:      "golang-checkins",
			startDateTime:  startDateTimeIdsDate,
			endDateTime:    endDateTimeIdsDate,
			dom:            exTopicIdDomDate,
			wantRawMsgURLMap: map[string][]string{
				"2018-09.txt": []string{"https://groups.google.com/forum/message/raw?msg=golang-checkins/8sv65_WCOS4/3Fc-diD_AwAJ"}},
			wantErr: nil,
		},
		{
			comparisonType: "Capture abuse flagged messages that were hidden.",
			org:            "",
			groupName:      "golang-checkins",
			dom:            exAbuseHiddenMsg,
			wantRawMsgURLMap: map[string][]string{
				"abuse.txt": []string{"https://groups.google.com/forum/message/raw?msg=golang-checkins/8sv65_WCOS4/3Fc-diD_AwAJ"}},
			wantErr: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotRawMsgURLMap, gotErr = topicIDToRawMsgUrlMap(test.org, test.groupName, test.startDateTime, test.endDateTime, test.dom); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if !reflect.DeepEqual(gotRawMsgURLMap, test.wantRawMsgURLMap) {
				t.Errorf("Result response does not match.\n got: %v\nwant: %v", gotRawMsgURLMap, test.wantRawMsgURLMap)
			}
		})
	}
}

// TODO verify the temp map is needed
func TestGetRawMsgURLWorker(t *testing.T) {
	var startDateTime, endDateTime time.Time

	tests := []struct {
		comparisonType string
		org            string
		groupName      string
		httpToDom      utils.HttpDomResponse
		topicToMsgMap  TopicIDToRawMsgUrlMap
		wantUrlMap     map[string][]string
		wantErr        error
	}{
		{
			comparisonType: "Test worker call and getting url map result.",
			groupName:      "totalTopicsLess",
			httpToDom:      utils.FakeHttpDomResponse,
			topicToMsgMap:  utils.FakeTopicIDToRawMsgUrlMap,
			wantUrlMap:     map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}},
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		topicURLJobs := make(chan string, 1)
		results := make(chan urlResults, 1)

		t.Run(test.comparisonType, func(t *testing.T) {
			go getRawMsgURLWorker(test.org, test.groupName, startDateTime, endDateTime, test.httpToDom, test.topicToMsgMap, topicURLJobs, results)

			topicURLJobs <- "rawMsgUrlWorker"
			close(topicURLJobs)

			gotResult := <-results

			if !errors.Is(gotResult.err, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotResult.err, test.wantErr)
			}
			if !reflect.DeepEqual(gotResult.urlMap, test.wantUrlMap) {
				t.Errorf("Raw message url response does not match.\n got: %v\nwant: %v", gotResult.urlMap, test.wantUrlMap)
			}
		})
	}
}

func TestListRawMsgURLsByMonth(t *testing.T) {
	var startDateTime, endDateTime time.Time
	rawMsg100 := map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}}
	for i := 0; i < 100; i++ {
		rawMsg100["1893-01.txt"] = append(rawMsg100["1893-01.txt"], "https://en.wikipedia.org/wiki/Lili%CA%BBuokalani")
	}

	var (
		gotRawMsgURLMap map[string][]string
		gotErr          error
	)

	tests := []struct {
		comparisonType   string
		org              string
		groupName        string
		worker           int
		httpToDom        utils.HttpDomResponse
		topicToMsgMap    TopicIDToRawMsgUrlMap
		allDateRun       bool
		wantRawMsgURLMap map[string][]string
		wantErr          error
	}{
		{
			comparisonType:   "Pull topic ids for date 100+",
			org:              "",
			groupName:        "totalTopics100",
			worker:           1,
			httpToDom:        utils.FakeHttpDomResponse,
			topicToMsgMap:    utils.FakeTopicIDToRawMsgUrlMap,
			allDateRun:       true,
			wantRawMsgURLMap: rawMsg100,
			wantErr:          nil,
		},
		{
			comparisonType:   "Pull topic ids for date under 100",
			org:              "",
			groupName:        "totalTopicsLess",
			worker:           1,
			httpToDom:        utils.FakeHttpDomResponse,
			topicToMsgMap:    utils.FakeTopicIDToRawMsgUrlMap,
			allDateRun:       true,
			wantRawMsgURLMap: map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}},
			wantErr:          nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotRawMsgURLMap, gotErr = listRawMsgURLsByMonth(test.org, test.groupName, startDateTime, endDateTime, test.worker, test.httpToDom, test.topicToMsgMap, test.allDateRun); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if !reflect.DeepEqual(gotRawMsgURLMap, test.wantRawMsgURLMap) {
				t.Errorf("Result response does not match.\n got: %v\nwant: %v", gotRawMsgURLMap, test.wantRawMsgURLMap)
			}
		})
	}
}

func TestStoreTextWorker(t *testing.T) {

	ctx := context.Background()
	storage := utils.NewFakeStorageConnection("googlegroups")

	tests := []struct {
		comparisonType string
		org            string
		groupName      string
		httpToString   utils.HttpStringResponse
		chanInput      jobsData
		wantErr        error
	}{
		{
			comparisonType: "Test worker call and calling storage.",
			groupName:      "Lili'uokalani",
			httpToString:   utils.FakeHttpstringResponse,
			chanInput:      jobsData{topicURLList: []string{"rawMsgUrlWorker"}, fileName: "Lili'uokalani"},
			wantErr:        nil,
		},
		{
			comparisonType: "Test empty response string.",
			groupName:      "",
			httpToString:   utils.FakeHttpstringResponse,
			chanInput:      jobsData{topicURLList: []string{"", "rawMsgUrlWorker"}, fileName: "Lili'uokalani"},
			wantErr:        nil,
		},
		{
			comparisonType: "Test empty filename string.",
			groupName:      "",
			httpToString:   utils.FakeHttpstringResponse,
			chanInput:      jobsData{topicURLList: []string{"Lili'uokalani", "rawMsgUrlWorker"}, fileName: ""},
			wantErr:        emptyFileNameErr,
		},
	}
	for _, test := range tests {
		rawMsgsUrlJobs := make(chan jobsData, 1)
		results := make(chan error, 1)

		t.Run(test.comparisonType, func(t *testing.T) {
			go storeTextWorker(ctx, storage, test.httpToString, rawMsgsUrlJobs, results)

			rawMsgsUrlJobs <- test.chanInput
			close(rawMsgsUrlJobs)

			gotResult := <-results

			if !errors.Is(gotResult, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotResult, test.wantErr)
			}
		})
	}
}

func TestStoreRawMsgByMonth(t *testing.T) {

	ctx := context.Background()
	storage := utils.NewFakeStorageConnection("googlegroups")

	var (
		gotErr error
	)

	tests := []struct {
		comparisonType string
		org            string
		groupName      string
		worker         int
		httpToString   utils.HttpStringResponse
		msgResults     map[string][]string
		wantErr        error
	}{
		{
			comparisonType: "Test harness call to storage worker",
			org:            "",
			groupName:      "totalTopicsLess",
			worker:         1,
			httpToString:   utils.FakeHttpstringResponse,
			msgResults:     map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani", "https://www.biography.com/royalty/liliuokalani"}},
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotErr = storeRawMsgByMonth(ctx, storage, test.worker, test.msgResults, test.httpToString); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}

		})
	}
}
