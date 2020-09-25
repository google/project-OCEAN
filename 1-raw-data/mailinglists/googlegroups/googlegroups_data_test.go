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

package googlegroups

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/project-OCEAN/1-raw-data/gcs"
)

type fakeStorageConnection struct {
	gcs.StorageConnection
	ProjectID  string
	BucketName string
}

func newFakeStorageConnection() *fakeStorageConnection {
	return &fakeStorageConnection{ProjectID: "Susan-Picotte", BucketName: "Physician"}
}

// Simulate gcs StoreInBucket
func (gcs *fakeStorageConnection) StoreInBucket(ctx context.Context, fileName, url string) (storageErr error) {
	if strings.Contains(url, "Susan") {
		err := os.ErrNotExist
		storageErr = fmt.Errorf("%v", err)
	}
	return
}

func TestGetMonthYearKey(t *testing.T) {}
func TestGetTotalTopics(t *testing.T) {}
func TestGetToipcIDsFromUrl(t *testing.T) {}
func TestGetMsgIDsFromUrl(t *testing.T) {}
func TestListTopicIDListByMonth(t *testing.T) {}
func TestListRawMsgURLByMonth(t *testing.T) {}
func TestStoreRawMsgByMonth(t *testing.T) {}
func TestGetGoogleGroupsData(t *testing.T) {}