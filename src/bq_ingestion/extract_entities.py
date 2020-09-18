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

"""This script attempts to extract 'entities' from the mail archive information, by 
connecting the different names and emails associated with the same person. (People not only use
multiple emails across the years, but they sometimes change their display name). The script assumes
as input the two tables output by the `dataflow_names_emails.py` script. 
It uses some heuristics (under development) and will not be correct in all cases.
Currently the alg usually errs on the side of being conservative, so may not make all connections.
(E.g., different people use the same variant of a 'nospam' address, but we don't want to lump them
together under one entity. Similarly, many people will set their display name to just their first
name, e.g. "Bob", but we don't want to assume that all "Bob"s are the same person.) 
"""

import argparse
import pprint
import uuid
import re
import time

from google.cloud import storage
from google.cloud import bigquery

def chunks(l, n):
  """break a list into chunks"""
  for i in range(0, len(l), n):
    # Create an index range for l of n items:
    yield l[i:i+n]

def dump_entities(uids, fname='entities.oput'):
  with open(fname, 'w') as f:
    for k in uids:
      f.write('\n{}:\n'.format(k))
      f.write('{}\n'.format(uids[k]))

  # print('-----***---uids:')
  # pp = pprint.PrettyPrinter(indent=4)
  # pp.pprint(uids)

def get_emails_for_name(client, table_id, name, uid, uids, email_hash, name_hash):
  # print('\n*************at start of gefn for {}, email hash: {}'.format(name, email_hash))
  if not uid:
    uid = str(uuid.uuid1())
    uids[uid] = {'emails': set(), 'names': set([name])}
  if name in name_hash:
    nuids = name_hash[name]
    nuids.add(uid)
    name_hash[name] = nuids
  else:
    name_hash[name] = set([uid])
  entity = uids[uid]
  entity_emails = entity['emails']

  qname = name.replace("'", r"\'")
  query = "SELECT from_name, emails FROM {} where from_name = '{}'".format(table_id, qname)
  # print('trying query: {}'.format(query))
  query_job = client.query(query)
  try:
    for row in query_job:  # should just be 1?
      # print(f'row: {row}')
      print('in get_emails_for_name: {}, {}'.format(row['from_name'], row['emails']))
      for email in row['emails']:
        if not email in email_hash:
          print('email {} is new'.format(email))
          email_hash[email] = set([uid])
        else:  # we've already seen the email
          euids = email_hash[email]  # get its existing uids
          euids.add(uid)
          email_hash[email] = euids
        entity_emails.add(email)
  except Exception as err:  # TODO: ughh, clean this up
    print(err)
    qname = name.replace('"', r'\"')
    query = 'SELECT from_name, emails FROM {} where from_name = "{}"'.format(table_id, qname)
    print('okay, NOW trying query {}'.format(query))
    query_job = client.query(query)
    try:
      for row in query_job:  # should just be 1?
        print(f'row: {row}')
        print('in get_emails_for_name: {}, {}'.format(row['from_name'], row['emails']))
        for email in row['emails']:
          if not email in email_hash:
            print('email {} is new'.format(email))
            email_hash[email] = set([uid])
          else:  # we've already seen the email
            euids = email_hash[email]  # get its existing uids
            euids.add(uid)
            email_hash[email] = euids
          entity_emails.add(email)    
    except Exception as err2:
      print(err2)
      print(f'****failed to query for {name}')

  entity['emails'] = entity_emails
  uids[uid] = entity  # is this necessary?


def process_emails(args, client, table_id, uids, email_hash, name_hash):

  query = 'SELECT from_email, names FROM {}'.format(table_id)
  query_job = client.query(query)  # Make an API request.
  i = 0
  for row in query_job:
    print("****from_email={}, names={}".format(row['from_email'], row['names']))
    email = row['from_email']
    if not email in email_hash:  # then create new entity
      uid = str(uuid.uuid1())
      email_hash[email] = set([uid])
      # print('in xyz after new add, email hash: {}'.format(email_hash))
      print('***starting UID {} from email {}'.format(uid, email))
      uids[uid] = {'emails': set([email]), 'names': set(row['names'])}
    else:
      euids = email_hash[email]
      uid = list(euids)[0]  # get existing UID
      uid_emails = uids[uid]['emails']
      uid_names = uids[uid]['names']
      uids[uid] = {'emails': uid_emails.union(set([email])), 
          'names': uid_names.union(set(row['names']))}

    if 'spam' in email or 'foo' in email:
      name_uid = None
    else:
      name_uid = uid
    for name in row['names']:
      m = re.search('.*\s+.*', name)  # look for something suggestive of first & last name?...
      if (len(name) > 2 and m) or (not all(ord(char) < 128 for char in name)):  # strawman heuristic...
        if not name in name_hash:
          get_emails_for_name(client, args.emails_for_name_id, name, name_uid, uids,
              email_hash, name_hash)
        else:
          print('already found name {}, not reprocessing'.format(name))
          # this 'should' be the same uid... right? ---> no, not any more?
          # nuids = name_hash[name]
          # print('***uid: {}, name uids: {}'.format(uid, nuids))
      else:
        print('not doing more processing for name {}'.format(name))

    i += 1
    if i % 10000 == 0:
      dump_entities(uids, fname='entities_{}.oput'.format(i))


def rows_to_bigquery(json_rows, table_id, chunk_size):
  """insert a list of message dicts into the given BQ table.  chunk_size determines how many
  are loaded at once. (If the payload is too large, it will throw an error.)
  """
  client = bigquery.Client()
  table = client.get_table(table_id)
  jrcs = chunks(json_rows, chunk_size)
  for jl in jrcs:
    errors = client.insert_rows_json(table, jl)
    if errors == []:
      print("New rows have been added without error.")
    else:
      print(errors)
      print(jl)
    time.sleep(1)


def write_entities_to_bq(uids, table_id, chunk_size):
  # create list of json dicts
  json_rows = []
  for k in uids:
    jrow = {'uid': k, 'names': list(uids[k]['names']), 'emails': list(uids[k]['emails'])}
    json_rows.append(jrow)
  rows_to_bigquery(json_rows, table_id, chunk_size)


def main():
  argparser = argparse.ArgumentParser()
  # table_id = "your-project.your_dataset.your_table"
  argparser.add_argument('--emails-for-name-id',
      default='aju-vtests2.mail_archives.emails_name_test3')
  argparser.add_argument('--names-for-email-id',
      default='aju-vtests2.mail_archives.names_email_test3')
  argparser.add_argument('--entities-table-id', required=True)
  argparser.add_argument('--chunk-size', type=int, default=200)
  argparser.add_argument('--ingest', default=False, action='store_true')
  argparser.add_argument('--no-ingest', dest='ingest', action='store_false')  
  args = argparser.parse_args()

  uids = {}
  name_hash = {}
  email_hash = {}

  client = bigquery.Client()
  process_emails(args, client, args.names_for_email_id, uids, email_hash, name_hash)

  dump_entities(uids)
  # temp testing
  # print('name_hash: {}\n'.format(name_hash))
  # print('email_hash: {}'.format(email_hash))
  if args.ingest:
    write_entities_to_bq(uids, args.entities_table_id, args.chunk_size)

if __name__ == "__main__":
  main()

# table schema for entities table should be:
# uid	STRING	REQUIRED	
# emails	STRING	REPEATED	
# names	STRING	REPEATED	