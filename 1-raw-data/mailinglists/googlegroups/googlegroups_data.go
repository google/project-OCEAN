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
This package is for loading Google Groups mailing list data types into Cloud Storage.

List of all topics / threads url format| LIFO:
topicListURLBase = "https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s"
https://groups.google.com/forum/?_escaped_fragment_=forum/[GROUP NAME]
Pass in numbers [1-100] to get list of results
- Note keep between 1-100 in size and grow accordingly 10-110, 201-300, etc
- Link only to topic and not post


List of all messages in a topic url format | FIFO:
topicDetailURLBase = "https://groups.google.com%s/forum/?_escaped_fragment_=topic/%s/%s"
https://groups.google.com/forum/?_escaped_fragment_=topic/[GROUP NAME]/[TOPIC ID]

Topic pages contain results in this format with message id which we want:
"https://groups.google.com/d/msg/%s/%s/%s" // group, topic, msg id


Raw mail message url format:
messageURLBase = "https://groups.google.com%s/forum/message/raw?msg=%s/%s/%s"
https://groups.google.com/forum/message/raw?msg=[GROUP NAME]/[TOPIC ID]/[MSG ID]


Atom links:
- Gets similar information where msgs focus on msg content and topics pulls labeled sections like Approvals
https://groups.google.com/forum/feed/[GROUP NAME]/msgs/atom.xml?num=100
https://groups.google.com/forum/feed/[GROUP NAME]/topics/atom.xml?num=50


RSS link with link to msg and topic:
https://groups.google.com/forum/feed/[GROUP NAME]/msgs/rss.xml?num=50
https://groups.google.com/forum/feed/[GROUP NAME]/topics/rss.xml?num=50

*/

package googlegroups

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

// TODO setup workers to speed up and split processing url calls
//TODO duplicate http gets - look at consolidating
// TODO setup gobal errors to pass and test

// Create month year key for topic list map
func getMonthYearKey(matchDate string) (dateKey, currentMonth string, err error){
	var tempDate time.Time
	dateSplit :=strings.Split(matchDate, "/")
	numDigMonthDay := fmt.Sprintf("%d%d",len(dateSplit[0]), len(dateSplit[1]))
	switch numDigMonthDay{
	case "11":
		if tempDate, err = time.Parse("1/2/06", matchDate); err != nil {
			err = fmt.Errorf("End date string conversion to DateTime threw an error: %v", err)
		}
	case "12":
		if tempDate, err = time.Parse("1/02/06", matchDate); err != nil {
			err = fmt.Errorf("End date string conversion to DateTime threw an error: %v", err)
		}
	case "21":
		if tempDate, err = time.Parse("01/2/06", matchDate); err != nil {
			err = fmt.Errorf("End date string conversion to DateTime threw an error: %v", err)
		}
	case "22":
		if tempDate, err = time.Parse("01/02/06", matchDate); err != nil {
			err = fmt.Errorf("End date string conversion to DateTime threw an error: %v", err)
		}
	}
	dateKey = fmt.Sprintf("%04d-%02d", tempDate.Year(), int(tempDate.Month()))
	currentMonth = fmt.Sprintf("%02d", int(tempDate.Month()))
	return
}

func getTotalTopics(url string) (totalTopics int, err error){
	regTotal, _ := regexp.Compile("[^<]*?([0-9]+) *- *([0-9]+) of ([0-9]+)[^<]*?")

	var (
		response                         *http.Response
	)

	response, err = http.Get(url)
	if err != nil {
		err = fmt.Errorf("HTTP response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	dom, _ := goquery.NewDocumentFromReader(response.Body)
	text := dom.Find("i").Text()
	if regTotal.MatchString(text) {
		match := regTotal.FindStringSubmatch(text)
		num1, _ := strconv.Atoi(match[1])
		num2, _ := strconv.Atoi(match[2])
		num3, _ := strconv.Atoi(match[3])
		numList := []int{num1, num2, num3}
		sort.Ints(numList)
		totalTopics = numList[2]
	}
	return
}

// Create list of topic ids grouped by month
func getToipcIDsFromUrl(url, group string) (EOF bool, result map[string][]string, err error) {

	result = make(map[string][]string)

	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/topic/%s", group))
	// Alternate time option [0-9]{1,2}:[0-9]{2}\s(AM|PM)
	regTime, _ := regexp.Compile("[0-1]{0,1}[0-9]{1,2}:[0-5][0-9] (AM|PM)")
	regDate, _ := regexp.Compile("[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")

	var (
		response                         *http.Response
		monthYearKey, dateToParse, currentMonth, topicID string

	)

	// Get url data - put in separate function ??
	response, err = http.Get(url)
	if err != nil {
		err = fmt.Errorf("HTTP response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	dom, _ := goquery.NewDocumentFromReader(response.Body)

	dom.Find("tr").Each(func(i int, row *goquery.Selection) {
		row.Find("td").Each(func(i int, cell *goquery.Selection) {
			topicIdURL, ok := cell.Find("a").Attr("href")
				if ok {
					if regTopicURL.MatchString(topicIdURL) {
						// Capture topic id
						topicID = path.Base(topicIdURL)
					}
				}
				// Capture date topic posted
			dateClass, _ := cell.Attr("class")
			if dateClass == "lastPostDate" {
				matchDate := cell.Text()
				// Because its a map, unable to skip getting the monthyearkey
				if regTime.MatchString(matchDate) {
					dateToParse = time.Now().Format("01/02/06")
				} else if regDate.MatchString(matchDate) {
					dateToParse = matchDate
				}
				if monthYearKey, currentMonth, err = getMonthYearKey(dateToParse); err != nil {
					err = fmt.Errorf("Defining month year string key for topic lists returned an error: %v", err)
					return
				}
				result[monthYearKey] = append(result[monthYearKey], topicID)
			}
		})
	})
	// Check if there are more pages to load
	dom.Find("a").Each(func(i int, links *goquery.Selection) {
		val := links.Text()
		if strings.Contains(val, "More topics") {
			EOF = true
		}
	})
	return
}

// Get message ids from topic pages and create list of raw msg urls by month
func getMsgIDsFromUrl(url, org, topicId, group string) (rawMsgUrl string, result map[string][]string, err error) {
	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/msg/%s", group))

	var (
		response                         *http.Response
		msgId string
	)

	response, err = http.Get(url)
	if err != nil {
		err = fmt.Errorf("HTTP response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	dom, _ := goquery.NewDocumentFromReader(response.Body)

	dom.Find("tr").Find("td").Find("a").Each(func(i int, aSelector *goquery.Selection) {
		msgUrl, _ := aSelector.Attr("href")

		if regTopicURL.MatchString(msgUrl) {
			msgId = path.Base(msgUrl)
			rawMsgUrl = fmt.Sprintf("https://groups.google.com%s/forum/message/raw?msg=%s/%s/%s", org, group, topicId, msgId)
			return
		}
	})
	return
}

// Get list of topic IDs by month
func listTopicIDListByMonth(org, groupName string) (topicResults map[string][]string, err error) {
	var (
		urlTopicList, nextUrl string
		tmpTopicResults map[string][]string
		EOF bool
		pageIndex, totalTopics, countTopics int
	)

	EOF = true
	pageIndex, countTopics = 0, 0
	topicResults = make(map[string][]string)
	tmpTopicResults = make(map[string][]string)

	urlTopicList = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s", org, groupName)
	if totalTopics, err = getTotalTopics(urlTopicList); err != nil {
		err = fmt.Errorf("Error getting the total expected topics: %v", err)
		return
	}

	for EOF && countTopics < totalTopics {
		nextUrl = fmt.Sprintf("%s[%d-%d]", urlTopicList, pageIndex+1, pageIndex+100)
		pageIndex = pageIndex + 100

		if EOF, tmpTopicResults, err = getToipcIDsFromUrl(nextUrl, groupName); err != nil {
			err = fmt.Errorf("Getting links returned an error: %v", err)
			return
		}
		for date, lst := range tmpTopicResults {
			topicResults[date]= append(topicResults[date], lst...)
			countTopics = countTopics + len(lst)
			// TODO remove this break - using for testing
			if date == "2020-08"{
				EOF = false
				break
			}
		}
	}
	if totalTopics == countTopics {
		log.Printf("All topics captured. Total topics are %d but only %d were captured url.", totalTopics, countTopics)

	} else {
		log.Printf("Not all topics were captured. Total topics are %d but only %d were captured url.", totalTopics, countTopics)
	}

	return
}

// Get urls for raw message text by month
func listRawMsgURLByMonth(org, groupName string, topicResults map[string][]string) (msgResults map[string][]string, err error) {

	var outputFileName, topicURL, msgURLs string
	msgResults = make(map[string][]string)

	for date, topicList := range topicResults {
		outputFileName = fmt.Sprintf("%s.txt", date)
		for _, topicID := range topicList {
			topicURL = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=topic/%s/%s", org, groupName, topicID)

			if msgURLs, _, err = getMsgIDsFromUrl(topicURL, org, string(topicID), groupName); err != nil {
				err = fmt.Errorf("Getting links returned an error: %v", err)
				return
			}
			msgResults[outputFileName] = append(msgResults[outputFileName], msgURLs)
		}
	}
	return
}

// Put message text by month into GCS
func storeRawMsgByMonth(ctx context.Context, storage gcs.Connection, msgResults map[string][]string) (err error) {

	var bodyBytes []byte

	for fileName, urlList := range msgResults {
		textStore := ""
		outputFile, err := os.Create(fileName)
		defer outputFile.Close()
		for _, msgURL := range urlList {
			response, err := http.Get(msgURL)
			if err != nil {
				err = fmt.Errorf("HTTP response returned an error: %v", err)
				return
			}
			defer response.Body.Close()

			if bodyBytes, err = ioutil.ReadAll(response.Body); err != nil {
				return fmt.Errorf("Reading http response failed: %v", err)
			}
			textStore = textStore + "/n" + string(bodyBytes)
		}
		if err = storage.StoreTextContentInBucket(ctx, fileName, textStore); err != nil {
			return fmt.Errorf("Storage failed: %v", err)
		}
	}
	return
}

func GetGoogleGroupsData(ctx context.Context, org, groupName string, storage gcs.Connection, workerNum int) (err error) {

	var topicResults, msgResults map[string][]string

	if topicResults, err = listTopicIDListByMonth(org, groupName); err!=nil {
		err = fmt.Errorf("Getting topic ID list returned an error: %v", err)
		return
	}
	if msgResults, err = listRawMsgURLByMonth(org, groupName, topicResults); err!=nil {
		err = fmt.Errorf("Getting raw message urls returned an error: %v", err)
		return
	}
	if err = storeRawMsgByMonth(ctx, storage, msgResults) ; err!=nil {
		err = fmt.Errorf("Storing text in GCS threw an error error: %v", err)
		return
	}
	return
}

