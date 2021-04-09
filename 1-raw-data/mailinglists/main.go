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
This package is for loading different mailing list data types into Cloud Storage.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/googlegroups"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/mailman"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/pipermail"
	"github.com/google/project-OCEAN/1-raw-data/utils"
)

var (
	//Below variables used if build run
	buildListRun = flag.Bool("build-list-run", false, "Use flag to run build list run vs manual command line run.")
	allListRun   = flag.Bool("all-list-run", false, "Use flag to get variables from command-line or do a full mailing list run or to do simple build test run of one mailing list.")
	allDateRun   = flag.Bool("all-date-run", false, "Use flag to get variables from command-line or do a full run")
	projectID    = flag.String("project-id", "", "GCP Project id.")

	//Below variables used if manual run
	bucketName   = flag.String("bucket-name", "mailinglists", "Bucket name to store files.")
	subDirectory = flag.String("subdirectory", "", "Subdirectory to store files. Enter 1 or more and use spaces to identify. CAUTION also enter the groupNames to load to in the same order.")
	mailingList  = flag.String("mailinglist", "", "Choose which mailing list to process either pipermail (default), mailman, googlegroups")
	groupNames   = flag.String("groupname", "", "Mailing list group name. Enter 1 or more and use spaces to identify. CAUTION also enter the buckets to load to in the same order.")
	startDate    = flag.String("start-date", "", "Start date in format of year-month-date and 4dig-2dig-2dig.")
	endDate      = flag.String("end-date", "", "End date in format of year-month-date and 4dig-2dig-2dig.")
	workerNum    = flag.Int("workers", 1, "Number of workers to use for goroutines.")
	subNames     []string

	mailListMap = map[string]string{
		"gg-angular":                     "2009-09-01",
		"gg-golang-announce":             "2011-05-01",
		"gg-golang-checkins":             "2009-11-01",
		" gg-golang-codereviews":         "2013-12-01",
		"gg-golang-dev":                  "2009-11-01",
		"gg-golang-nuts":                 "2009-11-01",
		"gg-nodejs":                      "2009-06-01",
		"mailman-python-announce-list":   "1999-04-01",
		"mailman-python-dev":             "1999-04-01",
		"mailman-python-ideas":           "2006-12-01",
		"pipermail-python-announce-list": "1999-04-01",
		"pipermail-python-dev":           "1995-03-01",
		"pipermail-python-ideas":         "2006-12-01",
		"pipermail-python-list":          "1999-02-01"}
)

func getData(ctx context.Context, storage gcs.Connection, httpToDom utils.HttpDomResponse, workerNum, numMonths int, mailingList, groupName, startDateString, endDateString string) {
	switch mailingList {
	//TODO add start and end dates to pipermail
	case "pipermail":
		if err := pipermail.GetPipermailData(ctx, storage, groupName, startDateString, endDateString, httpToDom); err != nil {
			log.Fatalf("Mailman load failed: %v", err)
		}
	case "mailman":
		if err := mailman.GetMailmanData(ctx, storage, groupName, startDateString, endDateString, numMonths); err != nil {
			log.Fatalf("Mailman load failed: %v", err)
		}
		//TODO add start and end dates to google groups
	case "gg":
		if err := googlegroups.GetGoogleGroupsData(ctx, "", groupName, startDateString, endDateString, storage, workerNum); err != nil {
			log.Fatalf("GoogleGroups load failed: %v", err)
		}
	default:
		log.Fatalf("Mailing list %v is not an option. Change the option submitted.", mailingList)
	}
}

func main() {
	var err error
	numMonths := 1
	httpToDom := utils.DomResponse
	flag.Parse()
	fmt.Printf(*projectID)

	//Setup Storage connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageConn := gcs.StorageConnection{
		ProjectID:  *projectID,
		BucketName: *bucketName,
	}
	if err := storageConn.ConnectClient(ctx); err != nil {
		log.Fatalf("Connect GCS failes: %v", err)
	}
	if err := storageConn.CreateBucket(ctx); err != nil {
		log.Fatalf("Create GCS Bucket failed: %v", err)
	}

	if *buildListRun {
		//Build run to load mailing list data
		now := time.Now()
		//Set variables in build that aren't coming in on command line
		groupName := ""

		if !*allListRun {
			log.Printf("Build test run with mailman")
			storageConn.SubDirectory = "mailman-python-announce-list"
			groupName = "python-announce-list"
			*startDate = now.AddDate(0, -1, 0).Format("2006-01-02")
			*endDate = now.AddDate(0, -1, 1).Format("2006-01-02")

			if err := mailman.GetMailmanData(ctx, &storageConn, groupName, *startDate, *endDate, numMonths); err != nil {
				log.Fatalf("Mailman test build load failed: %v", err)
			}
			return
		}

		// Run Build to load all mailing lists
		for subName, origStartDate := range mailListMap {
			startDateResult, endDateResult := "", ""
			storageConn.SubDirectory = subName
			*mailingList = strings.SplitN(subName, "-", 2)[0]
			groupName = strings.SplitN(subName, "-", 2)[1]

			if *allDateRun {
				//Load all months
				log.Printf("All Date Cloud Run")
				//Set start and end dates with first mailing list date and current end date
				if startDateResult, endDateResult, err = utils.FixDate(origStartDate, *endDate); err != nil {
					log.Fatalf("Date error: %v", err)
				}
			} else {
				//Set start and end dates split by one month
				if startDateResult, endDateResult, err = utils.SplitDatesByMonth(*startDate, *endDate, 1); err != nil {
					log.Fatalf("Date error: %v", err)
				}
				log.Printf("One Month Run All MailingLists")
				startDateResult = now.AddDate(0, -1, 0).Format("2006-01-02")
				endDateResult = now.Format("2006-01-02")
			}

			//Get mailing list data and store
			getData(ctx, &storageConn, httpToDom, *workerNum, numMonths, *mailingList, groupName, startDateResult, endDateResult)
		}
	} else {

		if *subDirectory != "" {
			subNames = strings.Split(*subDirectory, " ")
		}

		for idx, groupName := range strings.Split(*groupNames, " ") {
			//Apply sub directory name to storageConn if it exists
			if *subDirectory != "" {
				storageConn.SubDirectory = subNames[idx]
			}

			//Get mailing list data and store
			getData(ctx, &storageConn, httpToDom, *workerNum, numMonths, *mailingList, groupName, *startDate, *endDate)
		}
	}
}
