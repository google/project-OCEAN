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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

// TODO setup gobal errors to pass and test
// TODO setup so can pull specific dates

type Results struct {
	urlMap map[string][]string
	err    error
}

type ggData struct {
	topicURLList []string
	fileName     string
}

var totalTopics int

func httpStringResponse(url string) (responseString string, err error) {
	var (
		bodyBytes []byte
		response  *http.Response
	)

	response, err = http.Get(url)
	if err != nil {
		err = fmt.Errorf("HTTP string response returned an error: %v", err)
		return
	}
	defer response.Body.Close()

	if bodyBytes, err = ioutil.ReadAll(response.Body); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			log.Printf("HTTP string get broken pipe ignored for url: %s/n", url)
		} else {
			err = fmt.Errorf("Reading http response failed: %v", err)
		}
		return
	}

	responseString = string(bodyBytes)

	return
}

func httpDomResponse(url string) (dom *goquery.Document, err error) {
	var response *http.Response

	if response, err = http.Get(url); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			log.Printf("HTTP dom get broken pipe ignored for url: %s/n", url)
		} else {
			err = fmt.Errorf("HTTP dom response returned an error: %v", err)
		}
		return
	}
	defer response.Body.Close()

	if dom, err = goquery.NewDocumentFromReader(response.Body); err != nil {
		err = fmt.Errorf("Goquery dom conversion returned an error: %v", err)
		return
	}
	return
}

// Create month year key for topic list map
func getMonthYearKey(matchDate string) (dateKey string, err error) {
	var tempDate time.Time
	dateSplit := strings.Split(matchDate, "/")
	numDigMonthDay := fmt.Sprintf("%d%d", len(dateSplit[0]), len(dateSplit[1]))
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

	fmt.Printf("%v", tempDate)
	dateKey = fmt.Sprintf("%04d-%02d", tempDate.Year(), int(tempDate.Month()))
	return
}

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

//////////////////////////// NOT USING GOROUTINE ///////////////////////////////////////////////////////////

func getToipcIDsFromUrl(url, group string) (EOF bool, result map[string][]string, err error) {

	result = make(map[string][]string)

	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/topic/%s", group))
	// Alternate time option [0-9]{1,2}:[0-9]{2}\s(AM|PM)
	regTime, _ := regexp.Compile("[0-1]{0,1}[0-9]{1,2}:[0-5][0-9] (AM|PM)")
	regDate, _ := regexp.Compile("[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")

	var (
		response                           *http.Response
		monthYearKey, dateToParse, topicID string
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
				if monthYearKey, err = getMonthYearKey(dateToParse); err != nil {
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

// Get list of topic IDs by month
func listTopicIDListByMonth(org, groupName string) (topicResults map[string][]string, err error) {
	var (
		urlTopicList, nextUrl               string
		tmpTopicResults                     map[string][]string
		EOF                                 bool
		pageIndex, totalTopics, countTopics int
		dom                                 *goquery.Document
	)

	EOF = true
	pageIndex, countTopics = 0, 0
	topicResults = make(map[string][]string)
	tmpTopicResults = make(map[string][]string)

	urlTopicList = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s", org, groupName)
	//if totalTopics, err = getTotalTopics(urlTopicList); err != nil {
	//	err = fmt.Errorf("Error getting the total expected topics: %v", err)
	//	return
	//}

	//Get total topics
	if dom, err = httpDomResponse(urlTopicList); err != nil {
		return
	}

	if totalTopics, err = getTotalTopics(dom); err != nil {
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
			topicResults[date] = append(topicResults[date], lst...)
			countTopics = countTopics + len(lst)
		}
	}
	if totalTopics == countTopics {
		log.Printf("All topics captured. Total topics are %d but only %d were captured url.", totalTopics, countTopics)

	} else {
		log.Printf("Not all topics were captured. Total topics are %d but only %d were captured url.", totalTopics, countTopics)
	}

	return
}

//////////////////////////// USING GOROUTINE ///////////////////////////////////////////////////////////

// Create list of topic ids grouped by month
func getTopicIDsFromDom(groupName string, dom *goquery.Document) (result map[string][]string, err error) {

	result = make(map[string][]string)

	regTopicURL, _ := regexp.Compile(fmt.Sprintf("/d/topic/%s", groupName))
	// Alternate time option [0-9]{1,2}:[0-9]{2}\s(AM|PM)
	regTime, _ := regexp.Compile("[0-1]{0,1}[0-9]{1,2}:[0-5][0-9] (AM|PM)")
	regDate, _ := regexp.Compile("[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")

	var monthYearKey, dateToParse, topicID string

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
				if monthYearKey, err = getMonthYearKey(dateToParse); err != nil {
					err = fmt.Errorf("Defining month year string key for topic lists returned an error: %v", err)
					return
				}
				result[monthYearKey] = append(result[monthYearKey], fmt.Sprintf("https://groups.google.com/forum/?_escaped_fragment_=topic/%s/%s", groupName, topicID))
			}
		})
	})
	return
}

func getTopicID(groupName string, topicURLs <-chan string, results chan<- Results) {
	var (
		dom        *goquery.Document
		tmpResults map[string][]string
		err        error
	)
	tmpResults = make(map[string][]string)

	for url := range topicURLs {
		//fmt.Printf("Download %s\n", url)
		if dom, err = httpDomResponse(url); err != nil {
			results <- Results{err: err}
			return
		}

		time.Sleep(time.Second)

		if tmpResults, err = getTopicIDsFromDom(groupName, dom); err != nil {
			results <- Results{err: fmt.Errorf("Getting dom info returned an error: %v", err)}
			return
		}

	}
	results <- Results{urlMap: tmpResults, err: nil}
}

// Get list of topic IDs by month
func listTopicIDByMonth(org, groupName string) (topicIDMap map[string][]string, err error) {
	var (
		urlTopicList           string
		pageIndex, countTopics int
		dom                    *goquery.Document
	)

	pageIndex, countTopics = 0, 0
	topicIDMap = make(map[string][]string)

	urlTopicList = fmt.Sprintf("https://groups.google.com%s/forum/?_escaped_fragment_=forum/%s", org, groupName)

	// TODO take off the comments when ready to run fully

	//Get total topics
	if dom, err = httpDomResponse(urlTopicList); err != nil {
		return
	}

	if totalTopics, err = getTotalTopics(dom); err != nil {
		err = fmt.Errorf("Error getting the total expected topics: %v", err)
		return
	}

	//totalTopics = 200

	// TODO fix for less than 100 in the amount to pull
	topicURLs := make(chan string, totalTopics/100+1)
	results := make(chan Results)

	for i := 0; i < totalTopics/100; i++ {
		topicURLs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, pageIndex+1, pageIndex+100)
		pageIndex = pageIndex + 100
	}
	if totalTopics%100 > 0 {
		topicURLs <- fmt.Sprintf("%s[%d-%d]", urlTopicList, totalTopics-totalTopics%100, totalTopics)
	}
	close(topicURLs)
	for i := 0; i < totalTopics/100; i++ {
		go getTopicID(groupName, topicURLs, results)
	}
	for i := 0; i < totalTopics/100; i++ {
		output := <-results
		time.Sleep(time.Second * 5)
		if output.err != nil {
			err = output.err
			return
		}
		for date, lst := range output.urlMap {
			topicIDMap[date] = append(topicIDMap[date], lst...)
			countTopics = countTopics + len(lst)
		}
	}

	if totalTopics == countTopics {
		log.Printf("/n")
		log.Printf("All topics captured. Total topics captured are %d.", totalTopics)

	} else {
		log.Printf("/n")
		log.Printf("Not all topics were captured. Total topics are %d but only %d were captured.", totalTopics, countTopics)
	}
	return
}

///////////////////////////////////////////////////////////////////////////////////////

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

func getRawMsgURL(org, groupName string, topicURLs <-chan ggData, results chan<- Results) {
	var (
		rawMsgURL, topicID string
		tmpResults         map[string][]string
		dom                *goquery.Document
		err                error
	)
	tmpResults = make(map[string][]string)

	for urls := range topicURLs {
		time.Sleep(time.Millisecond * 200)
		for _, url := range urls.topicURLList {
			//fmt.Printf(":: Download %s\n", url)
			if dom, err = httpDomResponse(url); err != nil {
				results <- Results{err: err}
				return
			}

			topicID = path.Base(url)

			if rawMsgURL, err = getMsgIDsFromDom(org, topicID, groupName, dom); err != nil {
				results <- Results{err: fmt.Errorf("Getting links returned an error: %v", err)}
				return
			}
			tmpResults[urls.fileName] = append(tmpResults[urls.fileName], rawMsgURL)
		}
	}
	results <- Results{urlMap: tmpResults, err: nil}
}

// Get urls for raw message text by month
func listRawMsgURLByMonth(org, groupName string, topicResults map[string][]string) (msgResults map[string][]string, err error) {

	var countTopics int

	msgResults = make(map[string][]string)
	topicURLs := make(chan ggData, totalTopics)

	for date, topicURLList := range topicResults {
		topicURLs <- ggData{topicURLList, fmt.Sprintf("%s.txt", date)}
	}
	close(topicURLs)

	results := make(chan Results)
	for i := 1; i <= len(topicResults); i++ {
		go getRawMsgURL(org, groupName, topicURLs, results)
	}

	for i := 1; i <= len(topicResults); i++ {
		output := <-results
		if output.err != nil {
			err = output.err
			return
		}
		for filename, lst := range output.urlMap {
			msgResults[filename] = append(msgResults[filename], lst...)
			time.Sleep(time.Millisecond * 500)
			countTopics = countTopics + len(lst)
		}
	}

	if totalTopics == countTopics {
		log.Printf("All msg urls captured. Total topics captured are %d.", totalTopics)

	} else {
		log.Printf("Not all msg urls were captured. Total topics are %d but only %d were captured.", totalTopics, countTopics)
	}

	return
}

func storeText(ctx context.Context, storage gcs.Connection, rawMsgURLs <-chan ggData, results chan<- error) {
	var (
		responseString string
		err            error
	)

	for urls := range rawMsgURLs {
		textStore := ""
		time.Sleep(time.Millisecond)
		for _, msgURL := range urls.topicURLList {
			if responseString, err = httpStringResponse(msgURL); err != nil {
				results <- fmt.Errorf("HTTP error: %v", err)
				return
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
	// Note you have to pass in something to result otherwise it will hang waiting for a result if you only return
	return
}

// Put message text by month into GCS
func storeRawMsgByMonth(ctx context.Context, storage gcs.Connection, msgResults map[string][]string) (err error) {

	rawMsgURLs := make(chan ggData, totalTopics)

	for fileName, urlList := range msgResults {
		rawMsgURLs <- ggData{urlList, fileName}
	}
	close(rawMsgURLs)

	results := make(chan error)
	defer close(results)

	for i := 1; i <= len(msgResults); i++ {
		go storeText(ctx, storage, rawMsgURLs, results)
	}

	for i := 1; i <= len(msgResults); i++ {
		output := <-results
		if output != nil {
			err = output
			return
		}
	}

	log.Printf("Storage complete.")
	return
}

func GetGoogleGroupsData(ctx context.Context, org, groupName string, storage gcs.Connection, workerNum int) (err error) {

	var topicResults, msgResults map[string][]string

	//if topicResults, err = listTopicIDByMonth(org, groupName); err != nil {
	//	err = fmt.Errorf("Getting topic ID list returned an error: %v", err)
	//	return
	//}
	if topicResults, err = listTopicIDListByMonth(org, groupName); err != nil {
		err = fmt.Errorf("Getting topic ID list returned an error: %v", err)
		return
	}

	if msgResults, err = listRawMsgURLByMonth(org, groupName, topicResults); err != nil {
		err = fmt.Errorf("Getting raw message urls returned an error: %v", err)
		return
	}
	if err = storeRawMsgByMonth(ctx, storage, msgResults); err != nil {
		err = fmt.Errorf("Storing text in GCS threw an error error: %v", err)
		return
	}
	return
}
