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
Access and load Pipermail data.
*/

package pipermail

//TODO
// Add test of the specific page format expected and how to parse it
// Check the most recent file stored and pull only what isn't there
// Run this monthly at start of new month to pull all new data

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
	"github.com/google/project-OCEAN/1-raw-data/utils"
)

var (
	storageErr = errors.New("Storage failed")
)

// Get, parse and store Pipermail data in GCS.
func GetPipermailData(ctx context.Context, storage gcs.Connection, groupName string, httpToDom utils.HttpDomResponse) (storeErr error) {
	mailingListURL := fmt.Sprintf("https://mail.python.org/pipermail/%s/", groupName)

	var (
		dom *goquery.Document
		err error
	)

	if dom, err = httpToDom(mailingListURL); err != nil {
		return fmt.Errorf("HTTP dom error: %v", err)
	}

	dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
		filename, ok := s.Attr("href")
		if ok {
			check := strings.Split(filename, ".")
			len := len(check) - 1
			if check[len] == "gz" {
				if strings.Split(filename, ":")[0] != "https" {
					url := fmt.Sprintf("%v%v", mailingListURL, filename)
					if _, err = storage.StoreContentInBucket(ctx, filename, url, "url"); err != nil {
						// Each func interface doesn't allow passing errors?
						storeErr = fmt.Errorf("%w: %v", storageErr, err)
					}
				}
			}
		}
	})
	return storeErr
}

// TODO create func to create map of what is in bucket and then compare to what is pulled from site so only pull new files
//func getLatestFile(setup Setup){
//	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
//	defer cancel()
//
//	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
//		Prefix:    prefix,
//		Delimiter: delim,
//	})
//	for {
//		attrs, err := it.Next()
//		if err == iterator.Done {
//			break
//		}
//		if err != nil {
//			return fmt.Errorf("Bucket(%q).Objects(): %v", bucket, err)
//		}
//		log.Fprintln(w, attrs.Name)
//		setup.latestFile = attrs.Name
//	}
//}

func main() {
}
