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
	"log"
	"math"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/utils"
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

var (
	dateTimeParseErr = fmt.Errorf("string to DateTime")
	fileNameErr      = fmt.Errorf("defining filename")
	emptyFileNameErr = fmt.Errorf("empty filename")
	rawMsgWorkerErr  = fmt.Errorf("raw message worker")
	topicCaptureErr  = fmt.Errorf("topic capture")
	storageErr       = fmt.Errorf("Storage failed")
)

// Create month year filename for topic list map
func getFileName(matchDate string) (fileName string, err error) {
	var tempDate time.Time
	dateSplit := strings.Split(matchDate, "/")
	numDigMonthDay := fmt.Sprintf("%d%d", len(dateSplit[0]), len(dateSplit[1]))

	// Convert string based on 1 or 2 digit month or day
	switch numDigMonthDay {
	case "11":
		if tempDate, err = time.Parse("1/2/06", matchDate); err != nil {
			err = fmt.Errorf("%w single digits error: %v", dateTimeParseErr, err)
		}
	case "12":
		if tempDate, err = time.Parse("1/02/06", matchDate); err != nil {
			err = fmt.Errorf("%w single month and double day digit error: %v", dateTimeParseErr, err)
		}
	case "21":
		if tempDate, err = time.Parse("01/2/06", matchDate); err != nil {
			err = fmt.Errorf("%w double month and single day digiterror: %v", dateTimeParseErr, err)
		}
	case "22":
		if tempDate, err = time.Parse("01/02/06", matchDate); err != nil {
			err = fmt.Errorf("%w double month and double day digit error: %v", dateTimeParseErr, err)
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
func getTotalTopics(dom *goquery.Document) (totalTopics int) {
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
func getMsgIDsFromDom(org, topicId, groupName string, dom *goquery.Document) (rawMsgUrl string) {
	regTopicURL := regexp.MustCompile(fmt.Sprintf("/d/msg/%s", groupName))

	var msgId string

	msgUrl, _ := dom.Find("a").Attr("href")
	if regTopicURL.MatchString(msgUrl) {
		msgId = path.Base(msgUrl)
		rawMsgUrl = fmt.Sprintf("https://groups.google.com%s/forum/message/raw?msg=%s/%s/%s", org, groupName, topicId, msgId)
	}
	return
}

type TopicIDToRawMsgUrlMap func(string, string, *goquery.Document) (rawMsgUrlMap map[string][]string, err error)

// Parse topic ids from dom, get message ids and create raw message url map by year-month filename
func topicIDToRawMsgUrlMap(org, groupName string, topicDom *goquery.Document) (rawMsgUrlMap map[string][]string, err error) {

	var msgDom *goquery.Document

	rawMsgUrlMap = make(map[string][]string)

	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/topic/%s", groupName))
	// Alternate time option [0-9]{1,2}:[0-9]{2}\s(AM|PM)
	regTime, _ := regexp.Compile("[0-1]{0,1}[0-9]{1,2}:[0-5][0-9] (AM|PM)")
	regDate, _ := regexp.Compile("[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")

	var fileName, dateToParse, topicID, msgURL, rawMsgURL string

	topicDom.Find("tr").Each(func(i int, row *goquery.Selection) {
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
					err = fmt.Errorf("%w error: %v", fileNameErr, err)
					return
				}

				msgURL = fmt.Sprintf("https://groups.google.com/forum/?_escaped_fragment_=topic/%s/%s", groupName, topicID)

				if msgDom, err = utils.DomResponse(msgURL); err != nil {
					return
				}

				// Get the message ids from the links associated with each topic id
				rawMsgURL = getMsgIDsFromDom(org, topicID, groupName, msgDom)
				// Store the urls for the raw message content into a map grouped by year-month
				rawMsgUrlMap[fileName] = append(rawMsgUrlMap[fileName], rawMsgURL)
			}
		})
	})
	return
}

// Worker converting list of topic id urls to dom objects and converting topic ids into the raw message urls
func getRawMsgURLWorker(org, groupName string, httpToDom utils.HttpDomResponse, topictoMsgMap TopicIDToRawMsgUrlMap, topicURLJobs <-chan string, results chan<- urlResults) {
	var (
		topicDom                 *goquery.Document
		topicResults, tmpResults map[string][]string
		err                      error
	)
	topicResults = make(map[string][]string)
	tmpResults = make(map[string][]string)

	// Get the dom structure for the topic url page
	for topicUrl := range topicURLJobs {
		if topicDom, err = httpToDom(topicUrl); err != nil {
			results <- urlResults{err: err}
			return
		}

		// Get partial results with raw message urls grouped by year-month filename
		// Output map includes all urls from that page which can be up to 100 values
		if tmpResults, err = topictoMsgMap(org, groupName, topicDom); err != nil {
			results <- urlResults{err: fmt.Errorf("%w error: %v", rawMsgWorkerErr, err)}
			return
		}

		//Combine all raw msg urls results if there are more than one topicURL page reviewed
		for fileName, rawMsgURL := range tmpResults {
			topicResults[fileName] = append(topicResults[fileName], rawMsgURL...)
			log.Printf("%d filename results grabbed for file: %s.", len(rawMsgURL), fileName)
		}

	}
	results <- urlResults{urlMap: topicResults, err: nil}
	return
}

// Goroutine setup to get/consolidate list of raw message urls by year-month text filename for pages with lists of topic urls.
func listRawMsgURLsByMonth(org, groupName string, worker int, httpToDom utils.HttpDomResponse, topicToMsgMap TopicIDToRawMsgUrlMap) (rawMsgUrlMap map[string][]string, err error) {
	var (
		urlTopicList                        string
		pageIndex, countMsgs, totalMessages int
		dom                                 *goquery.Document
	)

	pageIndex, countMsgs = 0, 0
	rawMsgUrlMap = make(map[string][]string)

	urlTopicList = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s", org, groupName)

	//Get total topics to track all topics (e.g. messages) are pulled
	if dom, err = httpToDom(urlTopicList); err != nil {
		return
	}

	totalMessages = getTotalTopics(dom)
	if totalMessages > 100 {
		worker = int(math.Min(float64(worker), float64(totalMessages/100)))
	} else {
		worker = int(math.Min(float64(worker), float64(totalMessages%100)))
	}

	topicURLJobs := make(chan string, totalMessages/100+1)
	results := make(chan urlResults, totalMessages/100+1)
	defer close(results)

	for i := 0; i < worker; i++ {
		go getRawMsgURLWorker(org, groupName, httpToDom, topicToMsgMap, topicURLJobs, results)
	}

	// Loop over each page to pull all topic urls and setup jobs
	for i := 0; i < totalMessages/100; i++ {
		topicURLJobs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, pageIndex+1, pageIndex+100)
		pageIndex = pageIndex + 100
	}
	if totalMessages%100 > 0 {
		topicURLJobs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, totalMessages-totalMessages%100, totalMessages)
		//pageIndex = pageIndex + totalMessages%100
	}
	close(topicURLJobs)

	// Combine all raw msg urls under the same year month filename
	for i := 0; i < worker; i++ {
		rawMsgURLListOutput := <-results
		if rawMsgURLListOutput.err != nil {
			err = rawMsgURLListOutput.err
			return
		}
		for fileName, rawMsgURL := range rawMsgURLListOutput.urlMap {
			rawMsgUrlMap[fileName] = append(rawMsgUrlMap[fileName], rawMsgURL...)
			countMsgs = countMsgs + len(rawMsgURL)
			log.Printf("Final: %d filename results grabbed for file: %s.", len(rawMsgURL), fileName)
		}
	}

	if totalMessages == countMsgs || totalMessages+1 == countMsgs {
		log.Printf("All topics captured: total topics captured are %d.", totalMessages)

	} else {
		log.Printf("Failed to capture all: total topics are %d but only %d were captured.", totalMessages, countMsgs)
		//err = fmt.Errorf("%w failed to capture all: total topics are %d but only %d were captured.", topicCaptureErr, totalMessages, countMsgs)
		return
	}
	return
}

// Worker to get text blobs by year-month text filename and store into GCS
func storeTextWorker(ctx context.Context, storage gcs.Connection, httpToString utils.HttpStringResponse, rawMsgsUrlJobs <-chan jobsData, results chan<- error) {
	var (
		response string
		err      error
	)

	for urls := range rawMsgsUrlJobs {
		textStore := ""
		for _, msgURL := range urls.topicURLList {
			if response, err = httpToString(msgURL); err != nil {
				results <- fmt.Errorf("HTTP error: %v", err)
				return
			}
			if response == "" && msgURL == "" {
				log.Printf("Url and response was empty for filename: %s", urls.fileName)
			} else if response == "" {
				log.Printf("Response was empty for url: %s", msgURL)
			}
			textStore = textStore + "/n" + response
		}
		if _, err = storage.StoreContentInBucket(ctx, urls.fileName, textStore, "text"); err != nil {
			results <- fmt.Errorf("%w: %v", storageErr, err)
			return
		}
		if urls.fileName != "" {
			log.Printf("Storing %s", urls.fileName)
		} else {
			results <- fmt.Errorf("URL map filename threw an error: %w", emptyFileNameErr)
			return
		}
	}
	results <- nil

	return
}

// Goroutine to process getting full text and storing into GCS
func storeRawMsgByMonth(ctx context.Context, storage gcs.Connection, worker int, msgResults map[string][]string, httpToString utils.HttpStringResponse) (err error) {

	rawMsgsUrlJobs := make(chan jobsData, len(msgResults))
	results := make(chan error, len(msgResults))
	defer close(results)

	if worker > len(msgResults) {
		worker = len(msgResults)
	}

	for i := 0; i < worker; i++ {
		go storeTextWorker(ctx, storage, httpToString, rawMsgsUrlJobs, results)
	}

	for fileName, urlList := range msgResults {
		rawMsgsUrlJobs <- jobsData{urlList, fileName}
	}
	close(rawMsgsUrlJobs)

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

	var (
		messageURLResults map[string][]string
		httpToDom         utils.HttpDomResponse
		httpToString      utils.HttpStringResponse
		topicToMsgMap     TopicIDToRawMsgUrlMap
	)
	httpToDom = utils.DomResponse
	httpToString = utils.StringResponse
	topicToMsgMap = topicIDToRawMsgUrlMap

	if messageURLResults, err = listRawMsgURLsByMonth(org, groupName, workerNum, httpToDom, topicToMsgMap); err != nil {
		return
	}

	if err = storeRawMsgByMonth(ctx, storage, workerNum, messageURLResults, httpToString); err != nil {
		return
	}
	return
}
