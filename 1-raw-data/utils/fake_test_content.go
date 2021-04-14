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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

// Simulate StorageConnection struct for tests
type FakeStorageConnection struct {
	gcs.StorageConnection
	ProjectID  string
	BucketName string
}

// Create new fake StorageConnection object for tests based on the test being run
func NewFakeStorageConnection(testPackage string) *FakeStorageConnection {
	if testPackage == "pipermail" {
		return &FakeStorageConnection{ProjectID: "Pine-Leaf", BucketName: "Bíawacheeitchish"}
	} else if testPackage == "mailman" {
		return &FakeStorageConnection{ProjectID: "Susan-Picotte", BucketName: "Physician"}
	}
	return &FakeStorageConnection{}
}

// Simulate CheckFileExists
func (gcs *FakeStorageConnection) CheckFileExists(ctx context.Context, fileName string) (exists bool) {
	return true
}

// Simulate StoreGCS
func (gcs *FakeStorageConnection) StoreContentInBucket(ctx context.Context, fileName, content, source string) (testVerifyCopyCalled int64, err error) {

	if strings.Contains(content, "Leaf") {
		return
	} else if strings.Contains(content, "Susan") {
		err = fmt.Errorf("%s", "Susan")
		return
	} else if strings.Contains(content, "space") {
		err = fmt.Errorf("%s", "Storage")
		return
	}
	return
}

// Mock creating HTTP response body and return as a string for tests
func FakeHttpstringResponse(url string) (responseString string, err error) {
	responseString = url
	return
}

// Mock creating HTTP response body and return as a dom for tests
func FakeHttpDomResponse(url string) (dom *goquery.Document, err error) {
	var exDomResponse string

	switch url {
	case "https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopics100":
		exDomResponse = `
				<html>
				<head>
					<title>My document</title>
				</head>
					<i>Showing 1-100 of 100 topics</i>
				</html>
		`
	case "https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopicsLess":
		exDomResponse = `
				<html>
				<head>
					<title>My document</title>
				</head>
					<i>Showing 1-1 of 1 topics</i>
				</html>
		`
	case "https://groups.google.com/forum/?_escaped_fragment_=forum/totalTopicsMissBody":
		exDomResponse = `
				<html>
				<head>
					<title>My document</title>
				</head>
				</html>
		`
	case "topicIDToRawMsgUrlMapTime":
		exDomResponse = `
			<html>
			<table>
			<tr><td class="subject"><a href="https://groups.google.com/d/topic/golang-checkins/8sv65_WCOS4" title="[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists">[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists</a></td><td class="lastPostDate">11:20 AM</td></tr>
			</table>
			</html>
	`
	case "topicIDToRawMsgUrlMapDate":
		exDomResponse = `
			<html>
			<table>
			<tr><td class="subject"><a href="https://groups.google.com/d/topic/golang-checkins/8sv65_WCOS4" title="[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists">[tools] internal/lsp/cache: use gopls.mod for the workspace module if it exists</a></td><td class="lastPostDate">9/27/18</td></tr>
			</table>
			</html>
	`
	case "abuseHiddenMsg":
		exDomResponse = `
			<html>
			<table>
			<tr><td class="subject"><a href="https://groups.google.com/d/topic/golang-checkins/8sv65_WCOS4" title="This topic has been hidden because it was flagged for abuse."><i>This topic has been hidden because it was flagged for abuse</i></a></td></tr>
			</table>
			</html>
	`
	case "msgIdsFromDom":
		exDomResponse = `
				<html>
				<table>
				<tr>
				<td class="subject"><a href="https://en.wikipedia.org/wiki/Lili%CA%BBuokalani/d/msg/queen/Kamakaʻeha/1891">Lydia Liliʻu Loloku Walania Kamakaʻeha</a></td>
				</tr>
				</table>
				</html>
		`
	case "rawMsgUrlWorker":
		exDomResponse = `<a href="https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"></a>`
	case "https://mail.python.org/pipermail/Pine-Leaf/":
		exDomResponse = `
			<html>
			<table>
			<tr><td><a href="1851-October-woman-chief.gz"</a></td></tr>
			</table>
			</html>
    `
	case "https://mail.python.org/pipermail/Space/":
		exDomResponse = `
			<html>
			<table>
			<tr><td><a href="1963-June-space.gz"</a></td></tr>
			</table>
			</html>
    `
	}
	return goquery.NewDocumentFromReader(strings.NewReader(exDomResponse))
}

// Mock creating raw message url map from topic ids for tests on Google Groups mailing list load
func FakeTopicIDToRawMsgUrlMap(org, groupName string, startDateTime, endDateTime time.Time, dom *goquery.Document) (rawMsgUrlMap map[string][]string, err error) {
	switch groupName {
	case "totalTopicsLess":
		rawMsgUrlMap = map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}}
	case "totalTopics100":
		rawMsgUrlMap = map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}}
		for i := 0; i < 100; i++ {
			rawMsgUrlMap["1893-01.txt"] = append(rawMsgUrlMap["1893-01.txt"], "https://en.wikipedia.org/wiki/Lili%CA%BBuokalani")
		}
	case "":
		rawMsgUrlMap = map[string][]string{"1893-01.txt": []string{"https://en.wikipedia.org/wiki/Lili%CA%BBuokalani"}}
	}
	return
}
