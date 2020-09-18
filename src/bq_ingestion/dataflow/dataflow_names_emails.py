# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dataflow pipeline to find all the names associated with an email, and all the emails associated
with a name, and output the results to two tables.
"""

from __future__ import absolute_import

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery


def create_set(values):
  res = set()
  for elt in values:
    if elt is not None and not elt == set([None]):
      res = res.union(elt)
  return res

def mf1(k_v):
  return {'from_name': k_v[0], 'emails': list(k_v[1]) if k_v[1] else []}


def mf2(k_v):
  return {'from_email': k_v[0], 'names': list(k_v[1]) if k_v[1] else []}


def get_emails(input_data):
  """..."""
  return (
      input_data
      | 'emails per name' >> beam.FlatMap(
          lambda row: [(row['from_name'], set([row['from_email']]))] if row['from_name'] and '@' in row['from_email'] else [])
      | 'name emails' >> beam.CombinePerKey(create_set)
      | 'format1' >>
      beam.Map(mf1)
      # beam.Map(lambda k_v: {
      #     'from_name': k_v[0], 'emails': list(k_v[1]) if k_v[1] else []
      # })
      )

def get_names(input_data):
  """..."""
  return (
      input_data
      | 'names per email' >> beam.FlatMap(
          lambda row: [(row['from_email'], set([row['from_name']]))] if row['from_email'] and '@' in row['from_email'] else [])
      | 'email names' >> beam.CombinePerKey(create_set)
      | 'format2' >>
      beam.Map(mf2)
      # beam.Map(lambda k_v: {
      #     'from_email': k_v[0], 'names': list(k_v[1]) if k_v[1] else []
      # })
      )

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='aju-vtests2:mail_archives.ingestion_test',
      help=(
          'Input BigQuery table to process specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  parser.add_argument(
      '--output1',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  parser.add_argument(
      '--output2',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

  parser.add_argument(
      '--gcs_location',
      required=False,
      help=('GCS Location to store files to load '
            'data into Bigquery'))

  known_args, pipeline_args = parser.parse_known_args(argv)
  table_schema1 = bigquery.TableSchema()
  field_schema = bigquery.TableFieldSchema()
  field_schema.name = 'from_name'
  field_schema.type = 'string'
  field_schema.mode = 'required'
  table_schema1.fields.append(field_schema)
  # repeated field
  field_schema = bigquery.TableFieldSchema()
  field_schema.name = 'emails'
  field_schema.type = 'string'
  field_schema.mode = 'repeated'
  table_schema1.fields.append(field_schema)

  table_schema2 = bigquery.TableSchema()
  field_schema = bigquery.TableFieldSchema()
  field_schema.name = 'from_email'
  field_schema.type = 'string'
  field_schema.mode = 'required'
  table_schema2.fields.append(field_schema)
  # repeated field
  field_schema = bigquery.TableFieldSchema()
  field_schema.name = 'names'
  field_schema.type = 'string'
  field_schema.mode = 'repeated'
  table_schema2.fields.append(field_schema)

  with beam.Pipeline(argv=pipeline_args) as p:

    # Read the table rows into a PCollection.
    rows = p | 'read' >> beam.io.ReadFromBigQuery(table=known_args.input)
    emails_per_name = get_emails(rows)
    names_per_email = get_names(rows)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    emails_per_name | 'Write1' >> beam.io.WriteToBigQuery(
        known_args.output1,
        # schema='from_name:STRING, emails:STRING',
        schema = table_schema1,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    names_per_email | 'Write2' >> beam.io.WriteToBigQuery(
        known_args.output2,
        schema = table_schema2,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

# example invocation. If output tables of the same name already exist, they will be dropped
# and overwritten.  You can use the 'DirectRunner' also, if you set
# GOOGLE_APPLICATION_CREDENTIALS locally.
# python names_emails.py \
#   --region $REGION \
#   --input 'project-ocean-281819:mail_archives.names_emails' \
#   --output1 'project-ocean-281819:mail_archives.emails_name_test2' \
#   --output2 'project-ocean-281819:mail_archives.names_email_test2' \
#   --runner DataflowRunner \
#   --project $PROJECT \
#   --temp_location gs://$BUCKET/tmp/
