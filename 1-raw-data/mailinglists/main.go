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
	//Variables required for build run
	codeRunType = flag.String("code-run-type", "buildTestRun", "Use flag to define which type configuration to run. Options are buildAllData, buildAllLatestMonthData, buildAllRangeDatesData, buildTestRun, manualRun.")
	projectID   = flag.String("project-id", "", "GCP Project id.")
	bucketName  = flag.String("bucket-name", "mailinglists", "Bucket name to store files.")

	//Optional variables depending on build or command line setup
	startDate = flag.String("start-date", "", "Start date in format of year-month-date and 4dig-2dig-2dig.")
	endDate   = flag.String("end-date", "", "End date in format of year-month-date and 4dig-2dig-2dig.")
	numMonths = flag.Int("months", 1, "Number of months to cover between start and end dates.")
	workerNum = flag.Int("workers", 20, "Number of workers to use for goroutines.")

	//Optional variables and best used with command line
	subDirectory = flag.String("subdirectory", "", "Subdirectory to store files. Enter 1 or more and use spaces to identify. CAUTION also enter the groupNames to load to in the same order.")
	mailingList  = flag.String("mailinglist", "", "Choose which mailing list to process either pipermail (default), mailman, googlegroups")
	groupNames   = flag.String("groupname", "", "Mailing list group name. Enter 1 or more and use spaces to identify. CAUTION also enter the buckets to load to in the same order.")
	subDirNames  []string

	mailListSubDirMap = map[string]string{
		"gg-angular":                     "2009-09-01",
		"gg-golang-announce":             "2011-05-01",
		"gg-golang-checkins":             "2009-11-01",
		"gg-golang-codereviews":          "2013-12-01",
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

func getData(ctx context.Context, storage gcs.Connection, httpToDom utils.HttpDomResponse, workerNum int, mailingList, groupName, startDateString, endDateString string, allDateRun bool) {
	switch mailingList {
	case "pipermail":
		if err := pipermail.GetPipermailData(ctx, storage, groupName, startDateString, endDateString, httpToDom); err != nil {
			log.Fatalf("Pipermail load failed: %v", err)
		}
	case "mailman":
		if err := mailman.GetMailmanData(ctx, storage, groupName, startDateString, endDateString); err != nil {
			log.Fatalf("Mailman load failed: %v", err)
		}
	case "gg":
		if err := googlegroups.GetGoogleGroupsData(ctx, "", groupName, startDateString, endDateString, storage, workerNum, allDateRun); err != nil {
			log.Fatalf("GoogleGroups load failed: %v", err)
		}
	default:
		log.Fatalf("Mailing list %v is not an option. Change the option submitted.", mailingList)
	}
}

func reviewFileNamesAndFixDates(ctx context.Context, mailingList, groupName, startDate, endDate string, storageConn gcs.Connection) (fileExists bool, startDateResult, endDateResult string, err error) {

	fileExists = true
	startDateResult, endDateResult = startDate, endDate

	for startDateResult < endDateResult && fileExists {
		//Advance start date if file exists
		if fileExists, startDateResult, err = createAndCheckFileNames(ctx, mailingList, groupName, startDateResult, true, storageConn); err != nil {
			err = fmt.Errorf("Looping start dates threw an error: %v", err)
		}

		//Reduce end date if file exists
		if fileExists, endDateResult, err = createAndCheckFileNames(ctx, mailingList, groupName, endDateResult, false, storageConn); err != nil {
			err = fmt.Errorf("Looping end dates threw an error: %v", err)
		}
	}
	return
}

func createAndCheckFileNames(ctx context.Context, mailingList, groupName, dateToCheck string, forwardDate bool, storageConn gcs.Connection) (fileExists bool, dateResult string, err error) {
	var (
		fileName string
		dateT    time.Time
	)

	if fileName, err = utils.CreateFileName(mailingList, groupName, dateToCheck); err != nil {
		err = fmt.Errorf("Filename error: %v", err)
	}

	//Check if file exists
	fileExists = storageConn.CheckFileExists(ctx, fileName)

	//Increase startDate by a month if file exists
	if fileExists {
		if dateT, err = utils.GetDateTimeType(dateToCheck); err != nil {
			err = fmt.Errorf("Date in Main error: %v", err)
		}
		//Add or subtract a month depending on if start or end
		if forwardDate {
			dateResult = utils.AddMonth(dateT).Format("2006-01-02")
			log.Printf("Added one month to create the new date: %s.", dateResult)
		} else {
			dateResult = dateT.AddDate(0, -1, 0).Format("2006-01-02")
			log.Printf("Subtracted one month to create the new date: %s.", dateResult)
		}
	} else {
		dateResult = dateToCheck
	}
	return
}

func main() {
	var (
		err        error
		fileExists bool
	)
	httpToDom := utils.DomResponse
	startDateResult, endDateResult := "", ""
	now := time.Now()
	flag.Parse()

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
	//Check and create bucket if needed
	if err := storageConn.CreateBucket(ctx); err != nil {
		log.Fatalf("Create GCS Bucket failed: %v", err)
	}

	switch *codeRunType {
	case "buildTestRun":
		// Run Build to test with only mailman python announce list
		log.Printf("Build test run using python-announce-list in  Mailman.")

		groupName := "python-announce-list"
		subDirName := "mailman-python-announce-list"
		storageConn.SubDirectory = subDirName
		*startDate = now.AddDate(0, -1, 0).Format("2006-01-02")
		*endDate = now.AddDate(0, -1, 1).Format("2006-01-02")

		if fileExists, startDateResult, endDateResult, err = reviewFileNamesAndFixDates(ctx, *mailingList, groupName, startDateResult, endDateResult, &storageConn); err != nil {
			log.Fatalf("Checking fileName exists error: %v", err)
		}
		if !fileExists && startDateResult < endDateResult {
			if err := mailman.GetMailmanData(ctx, &storageConn, groupName, *startDate, *endDate); err != nil {
				log.Fatalf("Mailman test build load failed: %v", err)
			}
		}
		return
	case "buildAllData", "buildAllLatestMonthData", "buildAllRangeDatesData":
		log.Printf("Build all mailing lists.")
		groupName := ""
		allDateRun := true

		for subName, origStartDate := range mailListSubDirMap {
			storageConn.SubDirectory = subName
			*mailingList = strings.SplitN(subName, "-", 2)[0]
			groupName = strings.SplitN(subName, "-", 2)[1]
			// Set end date to 1st of current month

			switch *codeRunType {
			case "buildAllData":
				//Load all data from all mailing list group dates
				log.Printf("Load all dates and data.")
				//Set start and end dates with first mailing list date and current end date
				*endDate = utils.ChangeFirstMonth(now).Format("2006-01-02")
				if startDateResult, endDateResult, err = utils.FixDate(origStartDate, *endDate); err != nil {
					log.Fatalf("Date error: %v", err)
				}
			case "buildAllLatestMonthData":
				//Run Build to load most current month for all mailing lists
				log.Printf("Load last month.")
				*numMonths = 1
				//Remove pipermail-python-dev, .pipermail-python-announce-list, pipermail-python-ideas because no new data
				delete(mailListSubDirMap, "pipermail-python-dev")
				delete(mailListSubDirMap, "pipermail-python-announce-list")
				delete(mailListSubDirMap, "pipermail-python-ideas")
				//Set start and end dates split by one month
				*endDate = utils.ChangeFirstMonth(now).Format("2006-01-02")
				if startDateResult, endDateResult, err = utils.SplitDatesByMonth(*startDate, *endDate, *numMonths); err != nil {
					log.Fatalf("Date error: %v", err)
				}
			case "buildAllRangeDatesData":
				log.Printf("Load range of dates.")
				//Set start and end dates split by limited number of months
				if startDateResult, endDateResult, err = utils.SplitDatesByMonth(*startDate, *endDate, *numMonths); err != nil {
					log.Fatalf("Date error: %v", err)
				}
			}
			// Check and skip if file exists. Adjusts dates where files don't exist
			if fileExists, startDateResult, endDateResult, err = reviewFileNamesAndFixDates(ctx, *mailingList, groupName, startDateResult, endDateResult, &storageConn); err != nil {
				log.Fatalf("Checking fileName exists error: %v", err)
			}
			//Get mailinglist data and store
			if !fileExists && startDateResult < endDateResult {
				getData(ctx, &storageConn, httpToDom, *workerNum, *mailingList, groupName, startDateResult, endDateResult, allDateRun)
			}
		}
		return
	case "manualRun":
		//Manual run pulls variables from command line to load mailinglist group data
		log.Printf("Command line/manual run (not Build) to get mailing list data.")
		allDateRun := false
		if *subDirectory != "" {
			subDirNames = strings.Split(*subDirectory, " ")
		}
		if startDateResult, endDateResult, err = utils.FixDate(*startDate, *endDate); err != nil {
			log.Fatalf("Date error: %v", err)
		}

		for idx, groupName := range strings.Split(*groupNames, " ") {
			//Apply sub directory name to storageConn if it exists
			if *subDirectory != "" {
				storageConn.SubDirectory = subDirNames[idx]
			}
			// Check and skip if file exists. Adjusts dates where files don't exist
			if fileExists, startDateResult, endDateResult, err = reviewFileNamesAndFixDates(ctx, *mailingList, groupName, startDateResult, endDateResult, &storageConn); err != nil {
				log.Fatalf("Checking fileName exists error: %v", err)
			}
			//Get mailinglist data and store
			if !fileExists && startDateResult < endDateResult {
				getData(ctx, &storageConn, httpToDom, *workerNum, *mailingList, groupName, startDateResult, endDateResult, allDateRun)
			}
		}
		return
	}
}
