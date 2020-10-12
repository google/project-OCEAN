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
This package loads Google Groups mailing list data types into Cloud Storage.
It loops over pages that list topics and pulls the topic ids.
As it pulls each topic id, it explores the related link to get the message id.
Then it pulls the raw message content and groups that by month and year.
Then it stores the text on GCS.

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

Note Ajax crawling and escaped_fragment is deprecated and this will need to be revised to align to current approaches

*/

package googlegroups

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

// TODO setup gobal errors to pass and test
// TODO setup so can pull specific dates
// TODO setup to better handle different http errors and capturing where it fails

type urlResults struct {
	urlMap map[string][]string
	err    error
}

type jobsData struct {
	topicURLList []string
	fileName     string
}

// Create HTTP response body and return as a string
func httpStringResponse(url string) (responseString string, err error) {
	var (
		bodyBytes []byte
		response  *http.Response
	)

	// Keep program running even when url is empty. Returns emptry string and nil error
	if url == "" {
		return
	}

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("HTTP string response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	if bodyBytes, err = ioutil.ReadAll(response.Body); err != nil {
		//if errors.Is(err, syscall.EPIPE) {
		//	log.Printf("HTTP string get broken pipe ignored for url: %s/n", url)
		//} else {
		err = fmt.Errorf("Reading http response failed: %v", err)
		return
	}

	responseString = string(bodyBytes)
	return
}

// Create HTTP response body and return as a dom object
func httpDomResponse(url string) (dom *goquery.Document, err error) {
	var response *http.Response

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("HTTP dom response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	if dom, err = goquery.NewDocumentFromReader(response.Body); err != nil {
		err = fmt.Errorf("Goquery dom conversion returned an error: %v", err)
		return
	}
	return
}

// Create month year filename for topic list map
func getFileName(matchDate string) (fileName string, err error) {
	var tempDate time.Time
	dateSplit := strings.Split(matchDate, "/")
	numDigMonthDay := fmt.Sprintf("%d%d", len(dateSplit[0]), len(dateSplit[1]))

	// Convert string based on 1 or 2 digit month or day
	switch numDigMonthDay {
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
	// Found error in time.Parse of 2 date year that applies 20 to anything below 69. At time of thos code, anything after 2020 is future
	if tempDate.Year() > time.Now().Year() {
		tempDate = tempDate.AddDate(-100, 0, 0)
	}

	fileName = fmt.Sprintf("%04d-%02d.txt", tempDate.Year(), int(tempDate.Month()))
	return
}

// Get the total topics posted at the top of the list to track all are pulled
func getTotalTopics(dom *goquery.Document) (totalTopics int, err error) {
	regTotal, _ := regexp.Compile("[^<]*?([0-9]+) *- *([0-9]+) of ([0-9]+)[^<]*?")

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

// Get message ids from topic pages and create list of raw msg urls by month
func getMsgIDsFromDom(org, topicId, groupName string, dom *goquery.Document) (rawMsgUrl string, err error) {
	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/msg/%s", groupName))

	var msgId string

	msgUrl, _ := dom.Find("a").Attr("href")
	if regTopicURL.MatchString(msgUrl) {
		msgId = path.Base(msgUrl)
		rawMsgUrl = fmt.Sprintf("https://groups.google.com%s/forum/message/raw?msg=%s/%s/%s", org, groupName, topicId, msgId)
	}
	return
}

// Parse topic ids from dom, get message ids and create raw message url map by year-month filename
func topicIDToRawMsgUrlMap(org, groupName string, dom *goquery.Document) (rawMsgUrlMap map[string][]string, err error) {

	rawMsgUrlMap = make(map[string][]string)

	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/topic/%s", groupName))
	// Alternate time option [0-9]{1,2}:[0-9]{2}\s(AM|PM)
	regTime, _ := regexp.Compile("[0-1]{0,1}[0-9]{1,2}:[0-5][0-9] (AM|PM)")
	regDate, _ := regexp.Compile("[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")

	var fileName, dateToParse, topicID, msgURL, rawMsgURL string

	dom.Find("tr").Each(func(i int, row *goquery.Selection) {
		row.Find("td").Each(func(i int, cell *goquery.Selection) {
			topicIdURL, ok := cell.Find("a").Attr("href")
			if ok {
				// Get topic id
				if regTopicURL.MatchString(topicIdURL) {
					// Capture topic id
					topicID = path.Base(topicIdURL)
				}
			}
			// Capture date topic posted and convert to year-month text filename for grouping
			dateClass, _ := cell.Attr("class")
			if dateClass == "lastPostDate" {
				matchDate := cell.Text()
				// If the date is a time then its today otherwise parse the date
				if regTime.MatchString(matchDate) {
					dateToParse = time.Now().Format("01/02/06")
				} else if regDate.MatchString(matchDate) {
					dateToParse = matchDate
				}
				if fileName, err = getFileName(dateToParse); err != nil {
					err = fmt.Errorf("Defining month year string key for topic lists returned an error: %v", err)
					return
				}

				msgURL = fmt.Sprintf("https://groups.google.com/forum/?_escaped_fragment_=topic/%s/%s", groupName, topicID)

				if dom, err = httpDomResponse(msgURL); err != nil {
					return
				}

				// Get the message ids from the links associated with each topic id
				if rawMsgURL, err = getMsgIDsFromDom(org, topicID, groupName, dom); err != nil {
					return
				}
				// Store the urls for the raw message content into a map grouped by year-month
				rawMsgUrlMap[fileName] = append(rawMsgUrlMap[fileName], rawMsgURL)
			}
		})
	})
	return
}

// Worker converting list of topic ids urls to dom objects and converting topic ids into the raw message urls
func getRawMsgURLWorker(org, groupName string, topicURLJobs <-chan string, results chan<- urlResults) {
	var (
		dom                      *goquery.Document
		topicResults, tmpResults map[string][]string
		err                      error
	)
	topicResults = make(map[string][]string)
	tmpResults = make(map[string][]string)

	for url := range topicURLJobs {
		if dom, err = httpDomResponse(url); err != nil {
			results <- urlResults{err: err}
			return
		}

		if tmpResults, err = topicIDToRawMsgUrlMap(org, groupName, dom); err != nil {
			results <- urlResults{err: fmt.Errorf("Getting dom info returned an error: %v", err)}
			return
		}

		for fileName, rawMsgURL := range tmpResults {
			topicResults[fileName] = append(topicResults[fileName], rawMsgURL...)
			log.Printf("Filename results grabbed %s.", fileName)
		}

	}
	results <- urlResults{urlMap: topicResults, err: nil}
	return
}

// Goroutine setup to get/consolidate list of raw message urls by year-month text filename for pages with lists of topic urls.
func listRawMsgURLsByMonth(org, groupName string, worker int) (rawMsgUrlMap map[string][]string, err error) {
	var (
		urlTopicList                        string
		pageIndex, countMsgs, totalMessages int
		dom                                 *goquery.Document
	)

	pageIndex, countMsgs = 0, 0
	rawMsgUrlMap = make(map[string][]string)

	urlTopicList = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s", org, groupName)

	//Get total topics
	if dom, err = httpDomResponse(urlTopicList); err != nil {
		return
	}

	if totalMessages, err = getTotalTopics(dom); err != nil {
		err = fmt.Errorf("Error getting the total expected topics: %v", err)
		return
	}

	if worker > totalMessages/100 {
		worker = totalMessages / 100
	}

	topicURLJobs := make(chan string, totalMessages/100+1)
	results := make(chan urlResults, totalMessages/100+1)
	defer close(results)

	for i := 0; i < worker; i++ {
		go getRawMsgURLWorker(org, groupName, topicURLJobs, results)
	}

	for i := 0; i < totalMessages/100; i++ {
		topicURLJobs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, pageIndex+1, pageIndex+100)
		pageIndex = pageIndex + 100
	}
	if totalMessages%100 > 0 {
		topicURLJobs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, totalMessages-totalMessages%100, totalMessages)
	}
	close(topicURLJobs)

	for i := 0; i < worker; i++ {
		output := <-results
		if output.err != nil {
			err = output.err
			return
		}
		for fileName, rawMsgURL := range output.urlMap {
			rawMsgUrlMap[fileName] = append(rawMsgUrlMap[fileName], rawMsgURL...)
			countMsgs = countMsgs + len(rawMsgURL)
		}
	}

	if totalMessages == countMsgs || totalMessages+1 == countMsgs {
		log.Printf("All topics captured. Total topics captured are %d.", totalMessages)

	} else {
		log.Printf("Not all topics were captured. Total topics are %d but only %d were captured.", totalMessages, countMsgs)
	}
	return
}

// Worker to get text blobs by year-month text filename and store into GCS
func storeTextWorker(ctx context.Context, storage gcs.Connection, rawMsgURLs <-chan jobsData, results chan<- error) {
	var (
		responseString string
		err            error
	)

	for urls := range rawMsgURLs {
		textStore := ""
		for _, msgURL := range urls.topicURLList {
			if responseString, err = httpStringResponse(msgURL); err != nil {
				results <- fmt.Errorf("HTTP error: %v", err)
				return
			}
			if responseString == "" && msgURL == "" {
				log.Printf("Url and response was empty for filename: %s", urls.fileName)
			} else if responseString == "" {
				log.Printf("Response was empty for url: %s", msgURL)
			}
			textStore = textStore + "/n" + responseString
		}
		if err = storage.StoreTextContentInBucket(ctx, urls.fileName, textStore); err != nil {
			results <- fmt.Errorf("Storage failed: %v", err)
			return
		}
		log.Printf("Storing %s", urls.fileName)
	}
	results <- nil

	return
}

// Goroutine to process getting full text and storing into GCS
func storeRawMsgByMonth(ctx context.Context, storage gcs.Connection, worker int, msgResults map[string][]string) (err error) {

	rawMsgURLs := make(chan jobsData, len(msgResults))
	results := make(chan error, len(msgResults))
	defer close(results)

	if worker > len(msgResults) {
		worker = len(msgResults)
	}

	for i := 0; i < worker; i++ {
		go storeTextWorker(ctx, storage, rawMsgURLs, results)
	}

	for fileName, urlList := range msgResults {
		rawMsgURLs <- jobsData{urlList, fileName}
	}
	close(rawMsgURLs)

	for i := 0; i < worker; i++ {
		output := <-results
		if output != nil {
			err = output
			return
		}
	}

	log.Printf("Storage complete.")
	return
}

// Main function to run the script
func GetGoogleGroupsData(ctx context.Context, org, groupName string, storage gcs.Connection, workerNum int) (err error) {

	var messageURLResults map[string][]string

	if messageURLResults, err = listRawMsgURLsByMonth(org, groupName, workerNum); err != nil {
		err = fmt.Errorf("Getting topic ID list returned an error: %v", err)
		return
	}

	if err = storeRawMsgByMonth(ctx, storage, workerNum, messageURLResults); err != nil {
		err = fmt.Errorf("Storing text in GCS threw an error error: %v", err)
		return
	}
	return
}
