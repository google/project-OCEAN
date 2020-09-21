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
	"fmt"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

// Get, parse and store Pipermail data in GCS.
func GetPipermailData(ctx context.Context, storage gcs.Connection, mailingListURL string) (storageErr error) {
	response, err := http.Get(mailingListURL)

	if err != nil {
		return fmt.Errorf("HTTP response returned an error: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		dom, _ := goquery.NewDocumentFromReader(response.Body)
		dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
			filename, ok := s.Attr("href")
			if ok {
				check := strings.Split(filename, ".")
				len := len(check) - 1
				if check[len] == "gz" {
					if strings.Split(filename, ":")[0] != "https" {
						url := fmt.Sprintf("%v%v", mailingListURL, filename)
						if err := storage.StoreInBucket(ctx, filename, url); err != nil {
							// Each func interface doesn't allow passing errors?
							storageErr = fmt.Errorf("GCS storage failed: %w", err)
						}
					}
				}
			}
		})
	}
	return
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
