# Copyright 2021 Google Inc. All Rights Reserved.
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

from datetime import timezone
import email
import email.utils
import gzip
import os
import re
import time

import json

from dateutil import parser

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest

# TODO parallelize the code to run faster

ALLOWED_FIELDS = set(['from', 'subject', 'date', 'message_id', 'in_reply_to', 'references', 'body_text', 'body_html', 'body_image', 'mailing_list', 'to', 'cc', 'raw_date_string', 'log', 'content_type', 'filename', 'time_stamp', 'original_url', 'flagged_abuse'])
IGNORED_FIELDS = set(['delivered_to', 'received', 'mime_version', 'content_transfer_encoding'])

def get_filenames(storage_client, bucketname, filename=None, prefix=None):
    # Get list of filenames
    if filename:  # for testing: process just this file
        if prefix:
            return [prefix + "/" + filename]
        else:
            return [filename]
    return list_bucket_filenames(storage_client, bucketname, prefix)

def list_bucket_filenames(storage_client, bucketname, prefix):
    """Get gcs bucket filename list"""
    blobs = storage_client.list_blobs(bucketname, prefix=prefix, delimiter=None)
    return [blob.name for blob in blobs]

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
    print('Cannot decode blob in decode_message: {} based on error: {}\n'.format(blob, err))
    raise err

def decompress_line_by_line(blob, filenamepath, split_regex_value):
    # Include timestamp in local file name to avoid archive name clashes in case we're processing multiple buckets concurrently.
    message_lines, messages_list_result = [], []

    # TODO: this will break if the 'filename' includes a path. Check for/create parent dir first.
    # Pull down file locally to loop over
    temp_file = '/tmp/{}_{}'.format(int(time.time()), filenamepath)
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
    except Exception as err:
        print('{} not successfully gunzipped and throws error: {}'.format(filenamepath, err))
    # Delete temp file
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    return messages_list_result

def get_msgs_from_gcs(storage_client, bucketname, filenamepath):
    """Read a gcs file, and build an array of messages text. Returns the array of messages.
    """
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.get_blob(filenamepath)
    messages_blob, split_regex_value = "", ""
    message_lines, messages_list_result = [], []
    split_val = '[*****cut gobbled1gook*****]'
    try:
        if 'text/plain' in blob.content_type:
            # Parse and group messages using /n for specific cases in Golang messages
            split_regex_value = r'(\/n(.*?)(?:Received:|MIME-Version|X-Recieved:|X-BeenThere:|Date:))'
            add_split_val = '/n'
            messages_blob = blob.download_as_text()
            # Swap all Send reply to or Reply-to with In-Reply-To
            messages_blob = re.sub(r'^^Reply-To:', 'In-Reply-To:', messages_blob)
            # TODO dropping this now that the regex val is more specific but leaving in for now in case there is a need.
        elif 'application/x-gzip' in blob.content_type:
            #Parse and group messages using From in Python messages
            # Looks for split where there are two lines starting with From and the second has a :
            # Trying to ignore inline reponses in body when '> From:' exists and not capture From in the body message like 'From 1913 ...'
            split_regex_value = r'(From[^:].*\n?(?=From:))'
            add_split_val = ''
            #Unzip gzip and decode text
            message_bytes = gzip.decompress(blob.download_as_bytes())
            messages_blob = decode_messsage(message_bytes)
            # Swap all Send reply to or Reply-to with In-Reply-To
            messages_blob = re.sub(r'^^Send reply to:', 'In-Reply-To:', messages_blob)
    except UnicodeDecodeError as err:
        print("Getting GCS data Error: {}. Downloading and decompressing, go over file line by line to resolve.".format(err))
        messages_list_result = decompress_line_by_line(blob, filenamepath)
    except EOFError as err:
        # Catches a few 'empty' archives for which this error will be generated.
        print('Getting GCS data Error: {}. Not successfully gunzipped or empty and throws err.'.format(err))
    except AttributeError as err:
        # Catches a few 'empty' archives for which this error will be generated.
        print('Getting GCS data Error: {}. Check the file {} exists and spelled correctly.'.format(err, filenamepath))

    if not messages_list_result and split_regex_value and messages_blob:
        # Split messages into a list through regex, add unique split value, split on that unique value and filter for any empty values in list. Goal avoid edge cases and retain core info that are part of split regex value.
        messages_list_result = list(filter(None, re.sub(split_regex_value, split_val+"\\1", messages_blob).split(split_val+add_split_val)))

    return messages_list_result

def check_body_to(body_text):
    body_to = ""
    if body_text and re.search(r"^(.*?)wrote:", body_text):
        body_to = re.search(r"^(.*?)wrote:", body_text).groups()[0]
    if body_to and re.search(r"On.*[+,-]\d{2,4}?(?:[,,(\s)])", body_to):
        body_to = re.split(r"On.*[+,-]\d{2,4}?(?:[,,(\s)])", body_to)[1]
    return body_to.strip()

def parse_body(msg_object):
    """Given a parsed msg object, extract the text version of its body.
    """
    body_objects = []
    body_text, body_html, body_image= "", "", ""
    # Get body content and add to msg_parts_list
    if msg_object.is_multipart():
        for part in msg_object.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body_text += part.get_payload()
            if ctype == 'text/html':
                body_html += part.get_payload()
            if ctype == 'image/jpeg':
                body_image += part.get_payload()
    else:
        body_text = msg_object.get_payload()

    if body_text:
        body_objects.append(('body_text', body_text))
    if body_html:
        body_objects.append(('body_html', body_html))
    if body_image:
        body_objects.append(('body_image', body_image))

    body_to = check_body_to(body_text)
    if body_to:
        body_objects.append(('body_to', body_image))

    return body_objects

# TODO apply DLP to PII (email and names and any references that include email addresses) in: to, from, cc, in-reply-to, message-id, references
def get_msg_objs_list(msgs, bucketname, filenamepath):
    """Parse the msg texts into a list of header items per msg and pull out body"""
    msg_list = []

    for msg in msgs:
        if msg:  # then parse the message.
            msg_parts = []
            res = email.parser.Parser().parsestr(msg)
            msg_parts.extend(res.items())
            msg_parts.append(('mailing_list', bucketname))
            msg_parts.append(('filename', filenamepath.split("/")[1]))
            if "abuse" in filenamepath:
                msg_parts.append(('flagged_abuse', True))
            # Passing in time_stamp and AUTO to BQ generates an automatic timestamp for the row
            msg_parts.append(('time_stamp', 'AUTO'))
            # Capture original_url if it exists in message
            if "original_url:" in msg:
                val = re.split(r'original_url:', msg)
                msg_parts.append(('original_url', val[1]))
            msg_parts.extend(parse_body(res))
            msg_list.append(msg_parts)

    return msg_list

def convert_msg_to_json(msg_objects):
    """takes a list of message objects, and turns them into json dicts for insertion into BQ."""

    json_result = {'refs':[]} # this repeated field is not nullable

    msg_keys = {'date': parse_datestring, 'from': parse_contacts, 'to': parse_contacts, 'body_to': parse_contacts,'author': parse_contacts, 'cc': parse_contacts, 'references': parse_references}

    for (obj_key, obj_val) in msg_objects:
        # Parse fields identified as special
        if obj_val:
            if obj_key.lower() in msg_keys.keys():
                json_format_message_part = msg_keys[obj_key.lower()]((obj_key, obj_val))
            else:
                # Parse the rest of the fields
                json_format_message_part =  parse_everything_else((obj_key, obj_val))
        else:
            print("{} doesn't have a value from object: {}.".format(obj_key, msg_objects))
        json_result.update(json_format_message_part)

    return json_result

# Code to generate timezone map found at https://stackoverflow.com/questions/1703546/parsing-date-time-string-with-timezone-abbreviated-name-in-python/4766400#4766400
def get_timezone_map():
    tz_str = '''-12 Y
    -11 X NUT SST
    -10 W CKT HAST HST TAHT TKT
    -9 V AKST GAMT GIT HADT HNY
    -8 U AKDT CIST HAY HNP PST PT
    -7 T HAP HNR MST PDT
    -6 S CST EAST GALT HAR HNC MDT
    -5 R CDT COT EASST ECT EST ET HAC HNE PET
    -4 Q AST BOT CLT COST EDT FKT GYT HAE HNA PYT
    -3 P ADT ART BRT CLST FKST GFT HAA PMST PYST SRT UYT WGT
    -2 O BRST FNT PMDT UYST WGST
    -1 N AZOT CVT EGT
    0 Z EGST GMT UTC WET WT
    1 A CET DFT WAT WEDT WEST
    2 B CAT CEDT CEST EET SAST WAST
    3 C EAT EEDT EEST IDT MSK
    4 D AMT AZT GET GST KUYT MSD MUT RET SAMT SCT
    5 E AMST AQTT AZST HMT MAWT MVT PKT TFT TJT TMT UZT YEKT
    6 F ALMT BIOT BTT IOT KGT NOVT OMST YEKST
    7 G CXT DAVT HOVT ICT KRAT NOVST OMSST THA WIB
    8 H ACT AWST BDT BNT CAST HKT IRKT KRAST MYT PHT SGT ULAT WITA WST
    9 I AWDT IRKST JST KST PWT TLT WDT WIT YAKT
    10 K AEST ChST PGT VLAT YAKST YAPT
    11 L AEDT LHDT MAGT NCT PONT SBT VLAST VUT
    12 M ANAST ANAT FJT GILT MAGST MHT NZST PETST PETT TVT WFT
    13 FJST NZDT
    11.5 NFT
    10.5 ACDT LHST
    9.5 ACST
    6.5 CCT MMT
    5.75 NPT
    5.5 SLT
    4.5 AFT IRDT
    3.5 IRST
    -2.5 HAT NDT
    -3.5 HNT NST NT
    -4.5 HLV VET
    -9.5 MART MIT'''
    tzd = {}
    for tz_descr in map(str.split, tz_str.split('\n')):
        tz_offset = int(float(tz_descr[0]) * 3600)
        for tz_code in tz_descr[1:]:
            tzd[tz_code] = tz_offset
    return tzd

# TODO: Python chat channel, confirmed this approach | alternative if/elif potential but need to confirm will not miss trying all options without throwing one exception that stops it
def parse_datestring(datestring):
    """Given a date string, parse date to the format year-month-dayThour:min:sec and convert to DATETIME-friendly utc time.
    All the different formats are probably due to ancient mail client variants. Older messages have issues.
    """
    datestring = datestring[1]
    date_objects = {}
    date_objects['raw_date_string'] = datestring.strip()
    tzd = get_timezone_map()

    try:
        formated_date = parser.parse(datestring, tzinfos=tzd)
    except (TypeError, parser._parser.ParserError) as err:
        print('Parsing error: {}. For datestring: {}. Trying alternatives.'.format(datestring, err))
        formated_date = datestring.replace('.', ':')
        try:
            if re.search(r'(.* [-+]\d{4}).*$', datestring):
                pass
            elif re.search(r'(.* [-+]\d{1,3}).*$', datestring):
                print("Datestring {} was missing full timezone format.".format(datestring))
                ds_list = datestring.split(" ")
                # len should be 5 including the +/- sign
                num_zero_add = 5 - len(ds_list[-1])
                ds_list[-1] = ds_list[-1] + "0"*num_zero_add
                datestring = " ".join(ds_list)
            elif re.search(r'(.* \d{4}).*$', datestring):
                ds_list = datestring.split(" ")
                if ds_list[-1] == "0000" or ds_list[-1] == "0100":
                    ds_list[-1] = "+" + ds_list[-1]
                datestring = " ".join(ds_list)
            parsed_date = re.search(r'(.* [-+]\d{4}).*$', datestring)
            formated_date = parser.parse(parsed_date[1])
        except (TypeError, parser._parser.ParserError) as err2:
            print("Tried parse 2: (.* [-+]\d\d\d\d).*$ and got error: {}".format(err2))
            try:
                parsed_date = re.search(r'(.*)\(.*\)', datestring)
                formated_date = parser.parse(parsed_date[1])
            except (TypeError, parser._parser.ParserError) as err3:
                print("Tried parse 3: (.*)\(.*\) and got error: {}".format(err3))
                try:
                    parsed_date = re.search(r'(.*) [a-zA-Z]+$', datestring)
                    formated_date = parser.parse(parsed_date[1], tzinfos=tzd)
                except (TypeError, parser._parser.ParserError) as err4:
                    print("Tried parse 4: '(.*) [a-zA-Z]+$' and got error: {}".format(err4))
                    try:
                        formated_date = parser.parse(datestring, fuzzy=True)
                    except parser._parser.ParserError as err5:
                        print('**********Failed to parse datestring {} with error: {}'.format(datestring, err5))

    if type(formated_date) is not str:
        date_objects['date'] = formated_date.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        print("Formated date did not parse and was in this format: {}".format(formated_date))

    return date_objects

def parse_contacts(raw_contact):
    """Parse and convert from and to contact information in message"""
    to_from = raw_contact[0].lower().strip()
    raw_contact = raw_contact[1]
    contact_objects = {}
    contact_keys = {'from': ['raw_from_string', 'from_name', 'from_email'], 'to': ['raw_to_string','to_name','to_email'], 'body_to': ['raw_to_string','to_name','to_email'], 'author': ['raw_from_string', 'from_name', 'from_email'], 'cc': ['raw_cc_string','cc_name','cc_email'],}
    contact_string = raw_contact

    # Store raw from string after its decoded
    contact_objects[contact_keys[to_from][0]] = contact_string

    # Format from string and replace ' at ' syntax for pipermail email otherwise its ignored
    contact_string = contact_string.lower().replace(' at ', '@')
    if re.search(r'\([A-Za-z.].*@.*.com\)', contact_string):
        contact_string = contact_string.replace('(', '<').replace(')','>')
    if "@" not in contact_string:
        contact_string += "<>"

    # TODO add msg.get_all("from" or "to", []) if there are multiple & this can be clearer
    # Split out and store name and email
    parsed_addr = email.utils.getaddresses([contact_string])
    # TODO: better error checks/handling? The raw string will still be stored.
    try:
        # If email in first part then setup the matching
        if "@" in parsed_addr[0][0]:
            val_one, val_two = contact_keys[to_from][2], contact_keys[to_from][1]
        else:
            val_one, val_two = contact_keys[to_from][1], contact_keys[to_from][2]

        if parsed_addr[0][0]:
            contact_objects[val_one] = parsed_addr[0][0]
        if parsed_addr[0][1]:
            contact_objects[val_two] = parsed_addr[0][1]

    except IndexError as e:
        print("Broke parse on {}", parsed_addr)

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
    ref_objects['raw_refs_string'] = refs_string
    # TODO: there seems to be a rare case where there's info in parens following a ref, that prevents the regexp below from working properly. worth fixing?
    r1 = re.sub(r'>\s*<', '>|<', refs_string)
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
            # Remove  leading/trailing whitespace
            ee_objects[ee_key] = ee_raw.strip()
        except AttributeError as err:
            if type(ee_raw) == bool:
                ee_objects[ee_key] = ee_raw
            else:
                print('for *{}*, got error {} for {}'.format(ee_key, err, ee_raw))
                ee_objects[ee_key] = '{}'.format(ee_raw.strip())
    # elif ee_key in IGNORED_FIELDS:
    #     print('****Ignoring unsupported message field: {} in msg {}'.format(ee_key, ee_raw))

    return ee_objects

def chunks(json_rows, chunk_size):
    """break a list into chunks"""
    for indx in range(0, len(json_rows), chunk_size):
        # Create an index range for json_rows of chunk size of items:
        yield json_rows[indx:indx+chunk_size]

# TODO how to update and/or check for filename in table and insert if it doesn't exist...
def store_in_bigquery(client, json_rows, table_id, schema, num_rows_loaded=0):
    """Insert a list of message dicts into the given BQ table. If the payload is too large, it will throw an error.
    """

    try:
        # Try to get the table and if it doesn't exist, create it using the json format
        table = client.get_table(table_id)
        errors = client.insert_rows_json(table, json_rows)
        if errors:
            print("Loading messages rows did not load to BigQuery and threw this error: {}.".format(errors))
        else:
            num_rows_loaded += len(json_rows)
            print("{} rows or less have been added without error.".format(num_rows_loaded))
    except NotFound:
        with open(schema) as f:
            schema = json.load(f)
        table_framework = bigquery.Table(table_id, schema=schema)
        client.create_table(table_framework)
        table = client.get_table(table_id)
    except BadRequest:
        print("{} error thrown loading json_row to BigQuery. Trying to load again with reduced chunk size.".format(Exception))
        json_chunks = chunks(json_rows, len(json_rows)//2)
        for json_chunk in json_chunks:
            num_rows_loaded += store_in_bigquery(client, json_chunk, len(json_chunk), num_rows_loaded)
    return num_rows_loaded

def main(event, context):
    projectid = os.environ.get("PROJECT_ID")
    table_id = os.environ.get("TABLE_ID")
    schema = "../table_schema.json"

    bq_folders = {'angular': 'angular_mailinglist' , 'golang':'golang_mailinglist', 'nodejs': 'nodejs_mailinglist', 'python':'python_mailinglist'}
    bucketname = event['bucket']
    filepath = event['name'] # this is the Storage filename and use it for doc name
    prefix, filename = filepath.split("/")
    bq_prefix=prefix.split("-")[1]

    tableid = "{}.{}.{}".format(projectid, table_id, bq_folders[bq_prefix])
    print('----using table: {}----'.format(tableid))

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    filenamepaths = get_filenames(storage_client, bucketname, filename, prefix)

    for filenamepath in filenamepaths:
        print('Working on file: {}'.format(filenamepath))
        msgs_list = get_msgs_from_gcs(storage_client, bucketname, filenamepath)

        if msgs_list:
            msg_obj_list = get_msg_objs_list(msgs_list, bucketname, filenamepath)
            json_result = []
            # Create list of json dicts from parsed msg info
            result = list(map(convert_msg_to_json,msg_obj_list))
            json_result.extend(result)

            store_in_bigquery(bigquery_client, json_result, tableid, schema)

        else:
            print('*****No msgs obtained for {}'.format(filenamepath))

if __name__ == "__main__":
    main()