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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func TestGetHttpStringResponse(t *testing.T) {
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

			if gotResponseString, gotErr = httpStringResponse(test.url); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if strings.Compare(gotResponseString, test.wantResponseString) != 0 {
				t.Errorf("Filename response does not match.\n got: %v\nwant: %v", gotResponseString, test.wantResponseString)
			}
		})
	}
}

// TODO test for error?
func TestGetFileName(t *testing.T) {
	var gotDateKey string
	var gotErr error

	tests := []struct {
		comparisonType string
		matchDate      string
		wantFileName   string
		wantErr        error
	}{
		{
			// Confirm correct output with 1 dig month and 2 dig day
			comparisonType: "Single digit month and double digit day",
			matchDate:      "1/29/91",
			wantFileName:   "1991-01.txt",
			wantErr:        nil,
		},
		{
			// Confirm correct output with 1 dig month and 1 dig day and not future
			comparisonType: "Single digit month and single digit day",
			matchDate:      "9/2/38",
			wantFileName:   "1938-09.txt",
			wantErr:        nil,
		},
		{
			// Confirm correct output with 1 dig month and 2 dig day
			comparisonType: "Single digit month and double digit day",
			matchDate:      "1/24/95",
			wantFileName:   "1995-01.txt",
			wantErr:        nil,
		},
		{
			// Confirm correct output with 2 dig month and 2 dig day
			comparisonType: "Double digit month and double digit day",
			matchDate:      "11/11/17",
			wantFileName:   "2017-11.txt",
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotDateKey, gotErr = getFileName(test.matchDate); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if strings.Compare(gotDateKey, test.wantFileName) != 0 {
				t.Errorf("Filename response does not match.\n got: %v\nwant: %v", gotDateKey, test.wantFileName)
			}
		})
	}
}

func TestGetTotalTopics(t *testing.T) {
	exCorrectResponseBody := `
	<html>
	<head>
		<title>My document</title>
	</head>
		<i>Showing 1-100 of 1891 topics</i>
  </html>
		`
	exMissingResponseBody := `
	<html>
	<head>
		<title>My document</title>
	</head>
  </html>
		`
	exCorrectDom, _ := goquery.NewDocumentFromReader(strings.NewReader(exCorrectResponseBody))
	exMissingDom, _ := goquery.NewDocumentFromReader(strings.NewReader(exMissingResponseBody))

	var (
		gotTotalTopics int
		gotErr         error
	)

	tests := []struct {
		comparisonType  string
		dom             *goquery.Document
		wantTotalTopics int
		wantErr         error
	}{
		{
			comparisonType:  "Test regex to read total topics.",
			dom:             exCorrectDom,
			wantTotalTopics: 1891,
			wantErr:         nil,
		},
		{
			comparisonType:  "Test regex if info does not exist.",
			dom:             exMissingDom,
			wantTotalTopics: 0,
			wantErr:         nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotTotalTopics, gotErr = getTotalTopics(test.dom); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if gotTotalTopics != test.wantTotalTopics {
				t.Errorf("Total topic response does not match.\n got: %v\nwant: %v", gotTotalTopics, test.wantTotalTopics)
			}
		})
	}
}

func TestTopicIDToRawMsgUrlMap(t *testing.T) {
	timeMonthCheck, timeYearCheck := time.Now().Month(), time.Now().Year()

	exTopicIDResponseTime := `
	<html>
	<table>
	<tr><td class="subject"><a href="https://groups.google.com/d/topic/golang-checkins/8sv65_WCOS4" title="[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists">[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists</a></td><td class="lastPostDate">11:20 AM</td></tr>
	</table>
  </html>
`
	exTopicIDResponseDate := `
	<html>
	<table>
	<tr><td class="subject"><a href="https://groups.google.com/d/topic/golang-checkins/8sv65_WCOS4" title="[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists">[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists</a></td><td class="lastPostDate">9/27/18</td></tr>
	</table>
  </html>
`

	exTopicIdDomTime, _ := goquery.NewDocumentFromReader(strings.NewReader(exTopicIDResponseTime))
	exTopicIdDomDate, _ := goquery.NewDocumentFromReader(strings.NewReader(exTopicIDResponseDate))

	var (
		gotRawMsgURLMap map[string][]string
		gotErr          error
	)
	tests := []struct {
		comparisonType   string
		org              string
		group            string
		dom              *goquery.Document
		wantRawMsgURLMap map[string][]string
		wantErr          error
	}{
		{
			comparisonType: "Pull topic ids for time",
			org:            "",
			group:          "golang-checkins",
			dom:            exTopicIdDomTime,
			wantRawMsgURLMap: map[string][]string{
				fmt.Sprintf("%4d-%2d.txt", timeYearCheck, timeMonthCheck): []string{"https://groups.google.com/forum/message/raw?msg=golang-checkins/8sv65_WCOS4/3Fc-diD_AwAJ"}},
			wantErr: nil,
		},
		{
			comparisonType: "Pull topic ids for date",
			org:            "",
			group:          "golang-checkins",
			dom:            exTopicIdDomDate,
			wantRawMsgURLMap: map[string][]string{
				"2018-09.txt": []string{"https://groups.google.com/forum/message/raw?msg=golang-checkins/8sv65_WCOS4/3Fc-diD_AwAJ"}},
			wantErr: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotRawMsgURLMap, gotErr = topicIDToRawMsgUrlMap(test.org, test.group, test.dom); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if !reflect.DeepEqual(gotRawMsgURLMap, test.wantRawMsgURLMap) {
				t.Errorf("Result response does not match.\n got: %v\nwant: %v", gotRawMsgURLMap, test.wantRawMsgURLMap)
			}
		})
	}
}

func TestGetMsgIDsFromDom(t *testing.T) {
	exMsgIDResponse := `
	<html>
	<table>
	<tr>
  <td class="subject"><a href="https://en.wikipedia.org/wiki/Lili%CA%BBuokalani/d/msg/queen/Kamakaʻeha/1891">Lydia Liliʻu Loloku Walania Kamakaʻeha</a></td>
  </tr>
	</table>
  </html>
`

	exMsgIdDom, _ := goquery.NewDocumentFromReader(strings.NewReader(exMsgIDResponse))

	var (
		gotRawMsgUrl string
		gotErr       error
	)
	tests := []struct {
		comparisonType string
		org            string
		topicId        string
		group          string
		dom            *goquery.Document
		wantRawMsgUrl  string
		wantErr        error
	}{
		{
			comparisonType: "Output raw msg url",
			topicId:        "Kamakaʻeha",
			group:          "queen",
			dom:            exMsgIdDom,
			wantRawMsgUrl:  "https://groups.google.com/forum/message/raw?msg=queen/Kamakaʻeha/1891",
			wantErr:        nil,
		},
	}
	for _, test := range tests {
		t.Run(test.comparisonType, func(t *testing.T) {
			if gotRawMsgUrl, gotErr = getMsgIDsFromDom(test.org, test.topicId, test.group, test.dom); !errors.Is(gotErr, test.wantErr) {
				t.Errorf("Error response does not match.\n got: %v\nwant: %v", gotErr, test.wantErr)
			}
			if strings.Compare(gotRawMsgUrl, test.wantRawMsgUrl) != 0 {
				t.Errorf("DateKey response does not match.\n got: %v\nwant: %v", gotRawMsgUrl, test.wantRawMsgUrl)
			}
		})
	}
}

// TODO - fake http call and return value that is the format expected based on following exCorrectResponseBody
// TODO - test less than 100 and amount that is not divisable by 100
func TestListRawMsgURLsByMonth(t *testing.T) {}

func TestStoreRawMsgByMonth(t *testing.T) {}
