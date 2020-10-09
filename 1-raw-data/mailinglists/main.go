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

	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/googlegroups"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/mailman"
	"github.com/google/project-OCEAN/1-raw-data/mailinglists/pipermail"
)

var (
	projectID   = flag.String("project-id", "", "GCP Project id.")
	bucketNames = flag.String("bucket-name", "test", "Bucket name to store files. Enter 1 or more and use spaces to identify. CAUTION also enter the buckets to load to in the same order.")
	mailingList = flag.String("mailinglist", "piper", "Choose which mailing list to process either piper (default), mailman, googlegroups")
	groupNames  = flag.String("groupname", "", "Mailing list group name. Enter 1 or more and use spaces to identify. CAUTION also enter the buckets to load to in the same order.")
	startDate   = flag.String("start-date", "", "Start date in format of year-month-date and 4dig-2dig-2dig.")
	endDate     = flag.String("end-date", "", "End date in format of year-month-date and 4dig-2dig-2dig.")
	workerNum   = flag.Int("workers", 1, "Number of workers to use for goroutines.")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageConn := gcs.StorageConnection{
		ProjectID: *projectID,
	}

	if err := storageConn.ConnectClient(ctx); err != nil {
		log.Fatalf("Connect GCS failes: %v", err)
	}

	bNames := strings.Split(*bucketNames, " ")

	for idx, groupName := range strings.Split(*groupNames, " ") {

		storageConn.BucketName = bNames[idx]

		if err := storageConn.CreateBucket(ctx); err != nil {
			log.Fatalf("Create GCS Bucket failed: %v", err)
		}

		switch *mailingList {
		case "piper":
			if err := pipermail.GetPipermailData(ctx, &storageConn, groupName); err != nil {
				log.Fatalf("Mailman load failed: %v", err)
			}
		case "mailman":
			if err := mailman.GetMailmanData(ctx, &storageConn, groupName, *startDate, *endDate); err != nil {
				log.Fatalf("Mailman load failed: %v", err)
			}
		case "googlegroups":
			// TODO pass in org, group info and worker num
			if err := googlegroups.GetGoogleGroupsData(ctx, "", groupName, &storageConn, *workerNum); err != nil {
				log.Fatalf("GoogleGroups load failed: %v", err)
			}
		default:
			log.Fatalf("Mailing list %v is not an option. Change the option submitted.: ", mailingList)
		}
	}
}
