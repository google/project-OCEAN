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

import argparse
import base64
from datetime import timezone
import email
import email.utils
import gzip
# import json
import os
import re
import time

import chardet
from dateutil import parser

from google.cloud import storage
from google.cloud import bigquery

# TODO determine if clean up needed for message_id and in_reply_to
ALLOWED_FIELDS = ['from', 'subject', 'date', 'message_id', 'in_reply_to', 'references',
                  'body', 'list', 'to', 'cc', 'raw_date_string', 'body_bytes']
IGNORED_FIELDS = ['delivered_to', 'received', 'content_type', 'mime_version', 'content_transfer_encoding']

def list_bucket_filenames(storage_client, bucketname, prefix):
    """Get gcs bucket filename list"""
    blobs = storage_client.list_blobs(
        bucketname, prefix=prefix, delimiter=None)
    return [blob.name for blob in blobs]

def chunks(l, n):
    """break a list into chunks"""
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

# TODO is this needed: .encode('latin-1', errors='backslashreplace').decode('unicode-escape') as a way to convert special characters
def decode_messsage(blob, additional_codecs=[]):
    """Apply various codecs to decode a byte string"""
    codecs = ['utf8', 'iso8859_1', 'iso8859_2'] + [additional_codecs]
    err = None

    for codec in codecs:
        try:
            return blob.decode(codec)
        except (UnicodeDecodeError, LookupError, AttributeError) as e:
            err = e
    print('Cannot decode string: {} based on error: {}\n'.format(blob, err))
    raise err

def decompress_line_by_line(blob, fpath, split_regex_value):
    # Include timestamp in local file name to avoid archive name clashes in case we're processing multiple buckets concurrently.
    message_lines, messages_list_result = [], []

    # Pull down file locally to loop over
    temp_file = '/tmp/{}_{}'.format(int(time.time()), fpath)
    blob.download_to_filename(temp_file)

    try:
        with gzip.open(temp_file, 'rb') as encoded_file:
            for line in encoded_file:
                # Decode the line for re.search() call
                try:
                    decode_line = decode_messsage(line)
                # TODO: Unlikely to occur but catching if it does
                except UnicodeDecodeError as e:
                    print("{} error decoding line {}".format(e, line))
                    break
                split_point = re.search(split_regex_value, decode_line)
                if split_point:
                    # Combine message contents prior to From
                    messages_list_result.append(b''.join(message_lines))
                    # Place full msg into list
                    message_lines = [line]
                else:
                    message_lines.append(line)
            # Catches last message
            if message_lines:
                messages_list_result.append(b''.join(message_lines))
    except e:
        print(e)
        print('{} not successfully gunzipped'.format(fpath))
    # Delete temp file
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    return messages_list_result

# TODO: this will break if the 'filename' includes a path. Check for/create parent dir first.
def get_msgs_from_gcs(storage_client, bucketname, fpath):
    """Read a gcs file, and build an array of messages text. Returns the array of messages.
    """
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.get_blob(fpath)
    decode_by_line = False
    message_lines, messages_list_result = [], []
    try:
        if 'text/plain' in blob.content_type:
            # Parse and group messages using MIME for golang announce or Date for everything else from google groups text
            if "golang-announce" in bucketname:
                split_regex_value = '^^MIME.*\d'
            else:
                split_regex_value = '^^Date.*[\d)]'
            message_bytes = blob.download_as_text()
        elif 'application/x-gzip' in blob.content_type:
            #Parse and group messages using From as the start of the message for gzip
            split_regex_value = '^^From.*[\d>]'
            #Try to unzip gzip and decode text
            message_bytes = gzip.decompress(blob.download_as_bytes())
        messages_blob_list = decode_messsage(message_bytes)
    except UnicodeDecodeError as e:
        print("Hit error: %s. Downloading and decompressing, go over file line by line to resolve.".format(e))
        decode_by_line = True
    except EOFError as e:
        print(e)
        # Catches a few 'empty' archives for which this error will be generated.
        print('{} not successfully gunzipped or empty.'.format(fpath))

    # Split out messages into a list
    if decode_by_line:
        messages_list_result = decompress_line_by_line(blob, fpath)
    else:
        # TODO review whether encoding is needed or keep as string and use email parse by string - does this save space or speed?
        for line in messages_blob_list.split("\n"):
            split_point = re.search(split_regex_value, line)
            line = line.encode()
            if split_point and message_lines:
                # Combine message contents prior to split value
                # TODO drop the extra '\n' and/or drop '\n' altogether?
                messages_list_result.append(b'\n'.join(message_lines)+b'\n')
                # Place full msg into list
                message_lines = [line]
            else:
                message_lines.append(line)
        # Catches last message
        messages_list_result.append(b'\n'.join(message_lines))

    return messages_list_result

# TODO apply DLP to PII esp before appending body_bytes - to from email and names and any references that include email addresses
def get_msg_objs_list(msgs, bucketname):
    """Parse the msg texts into a list of header items per msg and pull out body"""
    msg_list = []

    for msg in msgs:
        if msg:  # then parse the message.
            msg_parts = []
            # TODO: is error-handling needed here? It doesn't appear to fail with the current archives.
            res = email.parser.BytesParser().parsebytes(msg)
            msg_parts.extend(res.items())
            msg_parts.append(('list', bucketname))
            msg_parts.extend(parse_body(res))
            msg_list.append(msg_parts)

    return msg_list

# TODO find better way to convert the base64 bytestring to a a string for the json bq ingestion
def encode_body(body):
    """
    Include base64-ified bytestring for the body to bigquery to provide backup in case decoding is corrupted. This may be redundant and if so remove
    """
    # it out)
    b64_bstring = base64.b64encode(body)
    return (str(b64_bstring)[2:])[:-1]


def parse_body(msg_object):
    """Given a parsed msg object, extract the text version of its body.
    """
    body_objects = []
    # Get body content and add to msg_parts_list
    if msg_object.is_multipart():
        for part in msg_object.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)
                break
    else:
        body = msg_object.get_payload(decode=True)

    body_objects.append(('Body', decode_messsage(body)))
    body_objects.append(('body_bytes', encode_body(body)))
    return body_objects

# TODO: this essentially works... but what's the best way to deal with all these different formats? (update: after discn on internal python chat channel, seems this may be the best approach...
#
def parse_datestring(datestring):
    """Given a date string, parse date to the format year-month-dayThour:min:sec and convert to DATETIME-friendly utc time.
    All the different formats are probably due to ancient mail client variants. Older messages have issues.
    """
    datestring = datestring[1]
    date_objects = {}

    try:
        formated_date = parser.parse(datestring)
        date_objects['raw_date_string'] = datestring.strip()
    except (TypeError, parser._parser.ParserError) as err:
        # print('date parsing error: {}'.format(err))
        formated_date = datestring.replace('.', ':')  # arghh/hmmm
        # print('---- parsing: {}'.format(datestring))

        if re.search('(.* [-+]\d{4}).*$', datestring):
            parsed_date = re.search('(.* [-+]\d{4}).*$', datestring)
            # print('tried: {}'.format('(.* [-+]\d\d\d\d).*$'))
            # print('trying date string {}'.format(m[1]))
            formated_date = parser.parse(parsed_date[1])
        elif re.search('(.*)\(.*\)', datestring):
            parsed_date = re.search('(.*)\(.*\)', datestring)
            # print('2nd try: {}'.format('(.*)\(.*\)'))
            # print('trying date string {}'.format(m[1]))
            formated_date = parser.parse(parsed_date[1])
        elif re.search('(.*) [a-zA-Z]+$', datestring):
            parsed_date = re.search('(.*) [a-zA-Z]+$', datestring)
            # print('3rd try: {}'.format('(.*) [a-zA-Z]+$'))
            # print('trying date string {}'.format(m[1]))
            formated_date = parser.parse(parsed_date[1])
        elif parser.parse(datestring, fuzzy=True):
            formated_date = parser.parse(datestring, fuzzy=True)
        else:
            print('**********Failed to parse datestring {} with error: {}'.format(datestring, err))
    except (AttributeError) as err:
        print('For "date", got error: {}'.format(err))
        revised_datestring = decode_messsage(email.header.decode_header(datestring)[0][0])
        date_objects['raw_date_string'] = revised_datestring.strip()
        formated_date = parse_datestring(revised_datestring)['date']

    if formated_date:
        date_objects['date'] = formated_date.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    return date_objects

# TODO add cc
def parse_contacts(raw_contact):
    """Parse and convert from and to contact information in message"""
    to_from = raw_contact[0].lower().strip()
    raw_contact = raw_contact[1]
    contact_objects = {}
    contact_keys = {'from': ['raw_from_string', 'from_name', 'from_email'], 'to': ['raw_to_string','to_name','to_email']}


    # TODO put all decoding when handling full message?
    # get_full_raw = email.header.decode_header(raw_contact)  # decode header
    #
    # if isinstance(get_full_raw[0], bytes):
    #     print('Decoded contact value: {}'.format(raw_contact))
    #     reconstruct_contact = b''
    #     enc = None
    #     # Decode content and recombine if needed
    #     for val in get_full_raw:
    #         reconstruct_contact += val[0].encode()
    #         if val[1] and not val[1] == 'unknown-8bit':
    #             enc = val[1]
    #             if enc == 'latin-2':
    #                 enc = 'iso-8859-2'  # sigh
    #             print('**************Got header encoded: {}'.format(enc))
    #     if not enc:
    #         enc = chardet.detect(reconstruct_contact)['encoding']
    #     if enc:
    #         contact_decoded = decode_messsage(reconstruct_contact, additional_codecs=[enc])
    #     else:
    #         contact_decoded = decode_messsage(reconstruct_contact)
    #     print('***For raw contact {}: and reconstructed {}, got encoding: {}\n with result {}'.format(raw_contact, reconstruct_contact, enc, contact_decoded))
    #     if contact_decoded:
    #         contact_string = contact_decoded
    #     else:  # hmmmm
    #         contact_string = '{}'.format(raw_contact)
    # else:
    contact_string = raw_contact

    # Store raw from string after its decoded
    contact_objects[contact_keys[to_from][0]] = contact_string

    # Format from string and replace ' at ' syntax for pipermail email otherwise its ignored
    contact_string = contact_string.lower().strip().replace(' at ', '@')

    # TODO add msg.get_all("from" or "to", []) if there are multiple
    # Split out and store name and email
    parsed_addr = email.utils.getaddresses([contact_string])
    # temp testing
    # if not parsed_addr[0][0]:
    #   print('---** problematic addr?')
    #   print('parsed_addr: {} from string {}'.format(parsed_addr, from_string))
    #   time.sleep(2)
    # TODO: better error checks/handling? The raw string will still be stored.
    if parsed_addr[0][0]:
        contact_objects[contact_keys[to_from][1]] = parsed_addr[0][0]
    if parsed_addr[0][1]:
        contact_objects[contact_keys[to_from][2]] = parsed_addr[0][1]

    return contact_objects

def parse_references(raw_reference):
    """Parse and convert reference information in message"""
    raw_reference = raw_reference[1]
    ref_objects = {'refs':[]} # this repeated field is not nullable

    try:
        refs_string = raw_reference.strip()
    except AttributeError as err:
        print('*******+++++++++++++++***********For {} got err: {}'.format(raw_reference, err))
        refs_string = '{}'.format(raw_reference)
        time.sleep(10)

    # Store reference strings
    ref_objects['references'] = refs_string
    # TODO: there seems to be a rare case where there's info in parens following a ref,
    # that prevents the regexp below from working properly. worth fixing?
    r1 = re.sub('>\s*<', '>|<', refs_string)
    refs = r1.split('|')
    # print('got refs: {}', refs)
    refs_record = [{"ref": x} for x in refs]
    ref_objects['refs'] = refs_record

    return ref_objects

def parse_everything_else(ee_raw):
    """Parse and convert all fields in ALLOWED_FIELDS in message"""
    # BQ fields allow underscores but not hyphens
    ee_key = ee_raw[0].lower().replace('-', '_')
    ee_raw = ee_raw[1]
    ee_objects = {}

    if ee_key in ALLOWED_FIELDS:
        try:
            ee_objects[ee_key] = ee_raw.lower().strip()  # get rid of any leading/trailing whitespace
        except AttributeError as err:
            print('for *{}*, got error {} for {}'.format(ee_key, err, ee_raw))
            print('trying decode method...')
            decode_ee = email.header.decode_header(ee_raw)
            print('dres: {}'.format(decode_ee))
            enc = chardet.detect(decode_ee[0][0])['encoding']
            print('got enc: {}'.format(enc))
            # TODO: do I need to handle substructure same as 'from' case above?
            if enc:
                ee_decoded = decode_messsage(decode_ee[0][0], additional_codecs=[enc])
            else:
                ee_decoded = decode_messsage(decode_ee[0][0])
            print('got decoded result: {}'.format(ee_decoded))
            if ee_decoded:
                ee_objects[ee_key] = ee_decoded.strip()
            else:
                ee_objects[ee_key] = '{}'.format(ee_raw.lower().strip())
            time.sleep(5)
    elif ee_key not in IGNORED_FIELDS:
        print('****Ignoring unsupported message field: {} in msg {}'.format(ee_key, ee_raw))
        # time.sleep(2)

    return ee_objects


def convert_msg_to_json(parsed_msg):
    """takes a list of message objects, and turns them into json dicts for insertion into BQ."""

    json_result = {'refs':[]} # this repeated field is not nullable

    msg_keys = {'date': parse_datestring, 'from': parse_contacts, 'to': parse_contacts, 'references': parse_references}

    for parts in parsed_msg:
        # Parse fields identified as special
        if parts[0].lower() in msg_keys.keys():
            json_result.update(msg_keys[parts[0].lower()](parts))
        else:
            # Parse the rest of the fields
            json_result.update(parse_everything_else(parts))

    return json_result


def messages_to_bigquery(json_rows, table_id, chunk_size):
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


def main():
    argparser = argparse.ArgumentParser(description='BQ message ingestion')
    argparser.add_argument('--bucketname', help='GCS bucketname to pull data from', required=True)

    argparser.add_argument('--filename', help='Optional, pass in single filename esp for testing or multiple. If left off then it pulls all filenames in bucket')
    # TODO use prefix to pull smaller groups of files if needed esp when running monthly
    argparser.add_argument('--prefix', defaul=None, help='Optional, to filter subdirectories and filenames based on prefix.')
    argparser.add_argument('--table-id', help="Required BigQuery table id to store files into in the format `your-project.your_dataset.your_table`", required=True)
    argparser.add_argument('--chunk-size', type=int, help='How many rows to load into BigQuery at a time.', default=200)
    argparser.add_argument('--ingest', default=False, help='Run the ingestion to BQ', action='store_true')
    argparser.add_argument('--no-ingest', dest='ingest', help='Do not run the ingestion to BQ. Esp for testing', action='store_false')
    args = argparser.parse_args()

    print('----using table: {}----'.format(args.table_id))
    # time.sleep(10)

    storage_client = storage.Client()

    # TODO pull all below into script to test
    # Get list of filenames
    if args.filename:  # for testing: process just this file
        fnames = args.filename.split(" ")
    else:
        fnames = list_bucket_filenames(storage_client, args.bucketname, args.prefix)

    for filename in fnames:
        print('---------------')
        print('Working on: {}'.format(filename))
        msgs_list = get_msgs_from_gcs(storage_client, args.bucketname, filename)

        if msgs_list:
            if args.bucketname.conaints('-gzip'):
                bucketName = args.bucketname.replace('-gzip', '')
            elif args.bucketname.conaints('-text'):
                bucketName = args.bucketname.replace('-text', '')

            msg_obj_list = get_msg_objs_list(msgs_list, bucketName)
            json_result = []
            # Create list of json dicts from parsed msg info
            json_result.append(map(convert_msg_to_json,msg_obj_list))

            # TODO remove - temp testing
            # with open('temp.oput', 'w') as f:
            #     for elt in json_result:
            #         f.write('\n----------\n')
            #         f.write('{}'.format(elt))

            if args.ingest:
                messages_to_bigquery(json_result, args.table_id, args.chunk_size)
                # time.sleep(1)

        else:
            print('*****No msgs obtained for {}'.format(filename))
            # time.sleep(5)


if __name__ == "__main__":
    main()
