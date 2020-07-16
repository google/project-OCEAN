# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''
This program enable querying BigQuery and outputting the results to Google Sheets. This was initially used to pull dataset metadata information for the public datasets the project is exploring.
Make sure the BQ results are a size that fits Google Sheets and the Sheet you are using
is in the same account as the one used to access the project and specifically BigQuery. Also make sure to enable Google Sheets API in Google Cloud project

To run this file run it with the python command and pass in arguments as described below under main.

In order to login and use BigQuery and Gspread for Google Sheets, you need credentials.
Here are a couple of options:

1. Run this in the GCP project's terminal or server and credentials will be handled there

OR

2. Use end user oauth https://cloud.google.com/docs/authentication/end-user.
Pass in location of credential json file (-s), project id (-p)

OR

3. Run locally using Service Accounts setup these environment variables.
export GOOGLE_APPLICATION_CREDENTIALS=*IAMFILE*.json
export GOOGLE_PROJECT='PROJECT_ID'

Get GOOGLE_APPLICATION_CREDENTIALS from project IAM Credentials Service

Change scopes as they make sense for your data needs. https://developers.google.com/identity/protocols/googlescopes

TODO add more logging for missing elements and errors
'''

from google.cloud import bigquery
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google_auth_oauthlib import flow
import gspread
import sys
import argparse
import logging

def get_credentials(secrets, local):
    appflow = flow.InstalledAppFlow.from_client_secrets_file(secrets, scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/logging.write", "https://www.googleapis.com/auth/spreadsheets"])

    if local:
        # Automatically set to run as host='localhost', port=8080
        appflow.run_local_server()
    else:
        appflow.run_console()

    return appflow.credentials


def setup_bq_client(credentials=None, projectid=None):
    if credentials:
        return bigquery.Client(project=projectid, credentials=credentials)
    return bigquery.Client()

def setup_sh_client(sheet_name, credentials=None, sheet_url=None):

    if credentials:
        gc = gspread.authorize(credentials)
        return gc.open_by_url(sheet_url)
        gc =  gspread.service_account()
    return gc.open(sheet_name)


def bq_to_sheet(bqclient, sheet, query, tabname, job_config=None):

    # Run query and output to dataframe
    df_schema = bqclient.query(query, job_config=job_config).to_dataframe()

    # If worksheet does not exist, create and add dataframe
    try:
        worksheet = sheet.add_worksheet(tabname, df_schema.shape[0], df_schema.shape[1])
        set_with_dataframe(worksheet, df_schema)
        print(worksheet)
    except Exception as err:
        logging.error(RuntimeError('**********Failed to log the error*********'), err)
    else:
        # If worksheet exists, get and append dataframe
        worksheet = sheet.worksheet(tabname)
        existing = get_as_dataframe(worksheet)
        updated = existing.append(df_schema, sort=False)
        set_with_dataframe(worksheet, updated)

    return f"Complete"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Function to query BigQuery and store it in Google Sheets.')

    parser.add_argument('-q','--query', help='BQ query',required=True, dest='query')
    parser.add_argument('-n','--sheetname', help='Google Sheet name', dest='sheet_name')
    parser.add_argument('-u','--sheeturl', help='Google Sheet url', dest='sheet_url')
    parser.add_argument('-t','--tabname', help='Google Sheet tab name to store the data.',required=True, dest='tabname')
    parser.add_argument('-j', '--jobconfig', help='Job configuration for the query if needed but not required', required=False, dest='job_config', default=None)
    parser.add_argument('-s', help='Json file that has the API credentials if setting up end user oauth https://cloud.google.com/docs/authentication/end-user', dest='secrets')
    parser.add_argument('-l','--local', help='If setting up oauth on your personal machine. It will help open a browswer window for authentication.', required=False, dest='local', type=bool , default=False)
    parser.add_argument('-p','--pi', help='Project ID', dest='projectid', default=None)

    par = parser.parse_args(sys.argv[1:])

    credentials = None
    if par.secrets:
        credentials = get_credentials(par.secrets, par.local)

    # Open BQ client
    bqclient = setup_bq_client(credentials, par.projectid)

    # Open Sheet client
    sheet = setup_sh_client(par.sheet_name, credentials, par.sheet_url)

    bq_to_sheet(bqclient, sheet, par.query, par.tabname, par.job_config)


