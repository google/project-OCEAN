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
	"1-raw-data/gcs"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"log"
	"net/http"
	"strings"
)

// Get, parse and store Pipermail data in GCS.
func GetMailingListData(storage gcs.StorageConnection, mailingListURL string) error {
	url := mailingListURL
	response, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP response returned an error: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		dom, _ := goquery.NewDocumentFromReader(response.Body)
		dom.Find("tr").Find("td").Find("a").Each(func(i int, s *goquery.Selection) {
			band, ok := s.Attr("href")
			if ok {
				check := strings.Split(band, ".")
				len := len(check) - 1
				if check[len] == "gz" {
					if strings.Split(band, ":")[0] != "https" {
						path := fmt.Sprintf("%v%v", mailingListURL, band)
						if err := storage.StoreGCS(band, path); err != nil {
							log.Fatalf("Storage failed: %v", err)
						}
					}
				}
			}
		})
	}
	return nil
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
