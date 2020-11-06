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

package utils

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/PuerkitoBio/goquery"
)

var (
	httpStrRespErr = fmt.Errorf("http string")
	httpDomRespErr = fmt.Errorf("http dom")
)

// Func pointer to create HTTP response body and return as a string
type HttpStringResponse func(string) (string, error)

// Create HTTP response body and return as a string
func StringResponse(url string) (responseString string, err error) {
	var (
		bodyBytes []byte
		response  *http.Response
	)

	// Keep program running even when url is empty. Returns emptry string and nil error
	if url == "" {
		return
	}

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("%w response returned an error: %v", httpStrRespErr, err)
		return
	}
	defer response.Body.Close()

	if bodyBytes, err = ioutil.ReadAll(response.Body); err != nil {
		//if errors.Is(err, syscall.EPIPE) {
		//	log.Printf("HTTP string get broken pipe ignored for url: %s/n", url)
		//} else {
		err = fmt.Errorf("%w reading bodybytes failed: %v", httpStrRespErr, err)
		return
	}

	responseString = string(bodyBytes)
	return
}

// Func pointer to create HTTP response body and return as a dom object
type HttpDomResponse func(string) (*goquery.Document, error)

// Create HTTP response body and return as a dom object
func DomResponse(url string) (dom *goquery.Document, err error) {
	var response *http.Response

	if response, err = http.Get(url); err != nil {
		err = fmt.Errorf("%w returned an error: %v", httpDomRespErr, err)
		return
	}
	defer response.Body.Close()

	if dom, err = goquery.NewDocumentFromReader(response.Body); err != nil {
		err = fmt.Errorf("%w goquery dom conversion returned an error: %v", httpDomRespErr, err)
		return
	}
	return
}
