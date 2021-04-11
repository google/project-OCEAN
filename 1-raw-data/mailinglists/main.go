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
	buildListRun = flag.Bool("build-list-run", false, "Use flag to run build list run vs manual command line run.")
	allListRun   = flag.Bool("all-list-run", false, "Load all mailing list data.")
	allDateRun   = flag.Bool("all-date-run", false, "Load all mailing list data for all dates")
	lastMonthRun = flag.Bool("last-month-run", false, "Load latest month data for mailing lists.")
	projectID    = flag.String("project-id", "", "GCP Project id.")
	bucketName   = flag.String("bucket-name", "mailinglist", "Bucket name to store files.")

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
)

func getData(ctx context.Context, storage gcs.Connection, httpToDom utils.HttpDomResponse, workerNum, numMonths int, mailingList, groupName, startDateString, endDateString string, allDateRun bool) {
	switch mailingList {
	case "pipermail":
		if err := pipermail.GetPipermailData(ctx, storage, groupName, startDateString, endDateString, httpToDom); err != nil {
			log.Fatalf("Pipermail load failed: %v", err)
		}
	case "mailman":
		if err := mailman.GetMailmanData(ctx, storage, groupName, startDateString, endDateString, numMonths); err != nil {
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

func main() {
	var err error
	httpToDom := utils.DomResponse
	startDateResult, endDateResult := "", ""
	flag.Parse()

	//Setup Storage connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageConn := gcs.StorageConnection{
		ProjectID: *projectID,
	}
	if err = storageConn.ConnectClient(ctx); err != nil {
		log.Fatalf("Connect GCS failes: %v", err)
	}

	if *buildListRun {
		//Build run to load mailing list data
		now := time.Now()
		//Set variables in build that aren't coming in on command line
		*bucketName = "mailinglists"
		groupName := ""

		// Setup bucket connection whether new or not
		storageConn.BucketName = *bucketName
		if err := storageConn.CreateBucket(ctx); err != nil {
			log.Fatalf("Create GCS Bucket failed: %v", err)
		}

		// Run Build to load all mailinglist groups
		if *allListRun {
			log.Printf("Build all lists ")
			// TODO add gcs and common func tests to run when loading - check if file for month exists and has data > than 1MB then capture - retry load if it fails
			mailListSubDirMap := map[string]string{"gg-angular": "2009-09-01", "gg-golang-announce": "2011-05-01", "gg-golang-checkins": "2009-11-01", "gg-golang-codereviews": "2013-12-01", "gg-golang-dev": "2009-11-01", "gg-golang-nuts": "2009-11-01", "gg-nodejs": "2009-06-01", "mailman-python-announce-list": "1999-04-01", "mailman-python-dev": "1999-04-01", "mailman-python-ideas": "2006-12-01", "pipermail-python-announce-list": "1999-04-01", "pipermail-python-dev": "1995-03-01", "pipermail-python-ideas": "2006-12-01", "pipermail-python-list": "1999-02-01"}

			for subName, origStartDate := range mailListSubDirMap {
				storageConn.SubDirectory = subName
				*mailingList = strings.SplitN(subName, "-", 2)[0]
				groupName = strings.SplitN(subName, "-", 2)[1]

				// Set end date to 1st of current month
				//TODO remove hardcode of the 1st
				*endDate = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")

				if *allDateRun {
					//Load all data from all mailing list group dates
					//log.Printf("All date run for all mailingLists.")
					//Set start and end dates with first mailing list date and current end date
					if startDateResult, endDateResult, err = utils.FixDate(origStartDate, *endDate); err != nil {
						log.Fatalf("Date error: %v", err)
					}
				} else if *lastMonthRun {
					//log.Printf("Last month run for all mailinglist groups.")
					// Hardcode 1 month since its only last month run
					*numMonths = 1
					//Set start and end dates split by one month
					if startDateResult, endDateResult, err = utils.SplitDatesByMonth(*startDate, *endDate, *numMonths); err != nil {
						log.Fatalf("Date error: %v", err)
					}
				} else {
					//log.Printf("Limited month run for all mailinglist groups.")
					//Set start and end dates split by limited number of months
					if startDateResult, endDateResult, err = utils.SplitDatesByMonth(*startDate, *endDate, *numMonths); err != nil {
						log.Fatalf("Date error: %v", err)
					}
				}
				log.Printf("Working on mailinglist group: %s", groupName)
				//Get mailinglist data and store
				getData(ctx, &storageConn, httpToDom, *workerNum, *numMonths, *mailingList, groupName, startDateResult, endDateResult, *allDateRun)
			}
		} else {
			log.Printf("Build test run with mailman")
			groupName = "python-announce-list"
			subDirName := "mailman-python-announce-list"
			storageConn.SubDirectory = subDirName
			*startDate = now.AddDate(0, -1, 0).Format("2006-01-02")
			*endDate = now.AddDate(0, -1, 1).Format("2006-01-02")

			if err := mailman.GetMailmanData(ctx, &storageConn, groupName, *startDate, *endDate, *numMonths); err != nil {
				log.Fatalf("Mailman test build load failed: %v", err)
			}
		}
	} else {
		//Manual run pulls variables from command line to load mailinglist group data
		log.Printf("Command line / non build mailinglist group run")
		if startDateResult, endDateResult, err = utils.FixDate(*startDate, *endDate); err != nil {
			log.Fatalf("Date error: %v", err)
		}

		storageConn.BucketName = *bucketName
		//Check and create bucket if needed
		if err := storageConn.CreateBucket(ctx); err != nil {
			log.Fatalf("Create GCS Bucket failed: %v", err)
		}

		if *subDirectory != "" {
			subDirNames = strings.Split(*subDirectory, " ")
		}

		for idx, groupName := range strings.Split(*groupNames, " ") {
			//Apply sub directory name to storageConn if it exists
			if *subDirectory != "" {
				storageConn.SubDirectory = subDirNames[idx]
			}

			log.Printf("Working on mailinglist group: %s", groupName)
			//Get mailinglist data and store
			getData(ctx, &storageConn, httpToDom, *workerNum, *numMonths, *mailingList, groupName, startDateResult, endDateResult, *allDateRun)
		}
	}
}
