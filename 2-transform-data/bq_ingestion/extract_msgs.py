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
import os
import re
import time

import json

from dateutil import parser

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest


timezone_info = {
    "A": 1 * 3600,
    "ACDT": 10.5 * 3600,
    "ACST": 9.5 * 3600,
    "ACT": -5 * 3600,
    "ACWST": 8.75 * 3600,
    "ADT": 4 * 3600,
    "AEDT": 11 * 3600,
    "AEST": 10 * 3600,
    "AET": 10 * 3600,
    "AFT": 4.5 * 3600,
    "AKDT": -8 * 3600,
    "AKST": -9 * 3600,
    "ALMT": 6 * 3600,
    "AMST": -3 * 3600,
    "AMT": -4 * 3600,
    "ANAST": 12 * 3600,
    "ANAT": 12 * 3600,
    "AQTT": 5 * 3600,
    "ART": -3 * 3600,
    "AST": 3 * 3600,
    "AT": -4 * 3600,
    "AWDT": 9 * 3600,
    "AWST": 8 * 3600,
    "AZOST": 0 * 3600,
    "AZOT": -1 * 3600,
    "AZST": 5 * 3600,
    "AZT": 4 * 3600,
    "AoE": -12 * 3600,
    "B": 2 * 3600,
    "BNT": 8 * 3600,
    "BOT": -4 * 3600,
    "BRST": -2 * 3600,
    "BRT": -3 * 3600,
    "BST": 6 * 3600,
    "BTT": 6 * 3600,
    "C": 3 * 3600,
    "CAST": 8 * 3600,
    "CAT": 2 * 3600,
    "CCT": 6.5 * 3600,
    "CDT": -5 * 3600,
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
    "CHADT": 13.75 * 3600,
    "CHAST": 12.75 * 3600,
    "CHOST": 9 * 3600,
    "CHOT": 8 * 3600,
    "CHUT": 10 * 3600,
    "CIDST": -4 * 3600,
    "CIST": -5 * 3600,
    "CKT": -10 * 3600,
    "CLST": -3 * 3600,
    "CLT": -4 * 3600,
    "COT": -5 * 3600,
    "CST": -6 * 3600,
    "CT": -6 * 3600,
    "CVT": -1 * 3600,
    "CXT": 7 * 3600,
    "ChST": 10 * 3600,
    "D": 4 * 3600,
    "DAVT": 7 * 3600,
    "DDUT": 10 * 3600,
    "E": 5 * 3600,
    "EASST": -5 * 3600,
    "EAST": -6 * 3600,
    "EAT": 3 * 3600,
    "ECT": -5 * 3600,
    "EDT": -4 * 3600,
    "EEST": 3 * 3600,
    "EET": 2 * 3600,
    "EGST": 0 * 3600,
    "EGT": -1 * 3600,
    "EST": -5 * 3600,
    "ET": -5 * 3600,
    "F": 6 * 3600,
    "FET": 3 * 3600,
    "FJST": 13 * 3600,
    "FJT": 12 * 3600,
    "FKST": -3 * 3600,
    "FKT": -4 * 3600,
    "FNT": -2 * 3600,
    "G": 7 * 3600,
    "GALT": -6 * 3600,
    "GAMT": -9 * 3600,
    "GET": 4 * 3600,
    "GFT": -3 * 3600,
    "GILT": 12 * 3600,
    "GMT": 0 * 3600,
    "GST": 4 * 3600,
    "GYT": -4 * 3600,
    "H": 8 * 3600,
    "HDT": -9 * 3600,
    "HKT": 8 * 3600,
    "HOVST": 8 * 3600,
    "HOVT": 7 * 3600,
    "HST": -10 * 3600,
    "I": 9 * 3600,
    "ICT": 7 * 3600,
    "IDT": 3 * 3600,
    "IOT": 6 * 3600,
    "IRDT": 4.5 * 3600,
    "IRKST": 9 * 3600,
    "IRKT": 8 * 3600,
    "IRST": 3.5 * 3600,
    "IST": 5.5 * 3600,
    "JST": 9 * 3600,
    "K": 10 * 3600,
    "KGT": 6 * 3600,
    "KOST": 11 * 3600,
    "KRAST": 8 * 3600,
    "KRAT": 7 * 3600,
    "KST": 9 * 3600,
    "KUYT": 4 * 3600,
    "L": 11 * 3600,
    "LHDT": 11 * 3600,
    "LHST": 10.5 * 3600,
    "LINT": 14 * 3600,
    "M": 12 * 3600,
    "MAGST": 12 * 3600,
    "MAGT": 11 * 3600,
    "MART": 9.5 * 3600,
    "MAWT": 5 * 3600,
    "MDT": -6 * 3600,
    "MHT": 12 * 3600,
    "MMT": 6.5 * 3600,
    "MSD": 4 * 3600,
    "MSK": 3 * 3600,
    "MST": -7 * 3600,
    "MEST": -7 * 3600,
    "MET": -7 * 3600,
    "MT": -7 * 3600,
    "MUT": 4 * 3600,
    "MVT": 5 * 3600,
    "MYT": 8 * 3600,
    "N": -1 * 3600,
    "NCT": 11 * 3600,
    "NDT": 2.5 * 3600,
    "NFT": 11 * 3600,
    "NOVST": 7 * 3600,
    "NOVT": 7 * 3600,
    "NPT": 5.5 * 3600,
    "NRT": 12 * 3600,
    "NST": 3.5 * 3600,
    "NUT": -11 * 3600,
    "NZDT": 13 * 3600,
    "NZST": 12 * 3600,
    "O": -2 * 3600,
    "OMSST": 7 * 3600,
    "OMST": 6 * 3600,
    "ORAT": 5 * 3600,
    "P": -3 * 3600,
    "PDT": -7 * 3600,
    "PET": -5 * 3600,
    "PETST": 12 * 3600,
    "PETT": 12 * 3600,
    "PGT": 10 * 3600,
    "PHOT": 13 * 3600,
    "PHT": 8 * 3600,
    "PKT": 5 * 3600,
    "PMDT": -2 * 3600,
    "PMST": -3 * 3600,
    "PONT": 11 * 3600,
    "PST": -8 * 3600,
    "PT": -8 * 3600,
    "PWT": 9 * 3600,
    "PYST": -3 * 3600,
    "PYT": -4 * 3600,
    "Q": -4 * 3600,
    "QYZT": 6 * 3600,
    "R": -5 * 3600,
    "RET": 4 * 3600,
    "ROTT": -3 * 3600,
    "S": -6 * 3600,
    "SAKT": 11 * 3600,
    "SAMT": 4 * 3600,
    "SAST": 2 * 3600,
    "SBT": 11 * 3600,
    "SCT": 4 * 3600,
    "SGT": 8 * 3600,
    "SRET": 11 * 3600,
    "SRT": -3 * 3600,
    "SST": -11 * 3600,
    "SYOT": 3 * 3600,
    "T": -7 * 3600,
    "TAHT": -10 * 3600,
    "TFT": 5 * 3600,
    "TJT": 5 * 3600,
    "TKT": 13 * 3600,
    "TLT": 9 * 3600,
    "TMT": 5 * 3600,
    "TOST": 14 * 3600,
    "TOT": 13 * 3600,
    "TRT": 3 * 3600,
    "TVT": 12 * 3600,
    "U": -8 * 3600,
    "ULAST": 9 * 3600,
    "ULAT": 8 * 3600,
    "UTC": 0 * 3600,
    "UYST": -2 * 3600,
    "UYT": -3 * 3600,
    "UZT": 5 * 3600,
    "V": -9 * 3600,
    "VET": -4 * 3600,
    "VLAST": 11 * 3600,
    "VLAT": 10 * 3600,
    "VOST": 6 * 3600,
    "VUT": 11 * 3600,
    "W": -10 * 3600,
    "WAKT": 12 * 3600,
    "WARST": -3 * 3600,
    "WAST": 2 * 3600,
    "WAT": 1 * 3600,
    "WEST": 1 * 3600,
    "WET": 0 * 3600,
    "WFT": 12 * 3600,
    "WGST": -2 * 3600,
    "WGT": -3 * 3600,
    "WIB": 7 * 3600,
    "WIT": 9 * 3600,
    "WITA": 8 * 3600,
    "WST": 14 * 3600,
    "WT": 0 * 3600,
    "X": -11 * 3600,
    "Y": -12 * 3600,
    "YAKST": 10 * 3600,
    "YAKT": 9 * 3600,
    "YAPT": 10 * 3600,
    "YEKST": 6 * 3600,
    "YEKT": 5 * 3600,
    "Z": 0 * 3600,
}

# TODO parallelize the code to run faster

ALLOWED_FIELDS = set(['from', 'subject', 'date', 'message_id', 'in_reply_to', 'references', 'body', 'mailing_list', 'to', 'cc', 'raw_date_string', 'body_bytes', 'log', 'content_type', 'filename'])
IGNORED_FIELDS = set(['delivered_to', 'received', 'mime_version', 'content_transfer_encoding'])

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
    print('Cannot decode blob in decode_message: {} based on error: {}\n'.format(blob, err))
    raise err

def decompress_line_by_line(blob, fpath, split_regex_value):
    # Include timestamp in local file name to avoid archive name clashes in case we're processing multiple buckets concurrently.
    message_lines, messages_list_result = [], []

    # TODO: this will break if the 'filename' includes a path. Check for/create parent dir first.
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

def get_msgs_from_gcs(storage_client, bucketname, fpath):
    """Read a gcs file, and build an array of messages text. Returns the array of messages.
    """
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.get_blob(fpath)
    messages_blob = ""
    message_lines, messages_list_result = [], []
    split_val = '[*****cut gobbled1gook*****]'
    try:
        if 'text/plain' in blob.content_type:
            # Parse and group messages using /n for specific cases in Golang messages
            # split_regex_value = '(\/n[RMX].*[\d;a-z])'
            split_regex_value = '(\/n(.*?)(?:Received:|MIME-Version|X-Recieved:|X-BeenThere:))'
            add_split_val = '/n'
            messages_blob = blob.download_as_text()
            # Swap all Send reply to or Reply-to with In-Reply-To
            messages_blob = re.sub(r'^^Reply-To:', 'In-Reply-To:', messages_blob)
            # TODO dropping this now that the regex val is more specific but leaving in for now in case there is a need.
            # Remove all html content that is a duplicate of the message content in Google Groups
            # messages_blob = re.sub(r'<html>((.|\n)*?)\/html>', '', messages_blob)
        elif 'application/x-gzip' in blob.content_type:
            #Parse and group messages using From in Python messages
            # split_regex_value = '(From(.*?)(?=(\d{2}):(\d{2}):(\d{2}) (\d{4})))'
            # Looks for split where there are two lines starting with From and the second has a :
            # Trying to ignore inline reponses in body when '> From:' exists and not capture From in the body message like 'From 1913 ...'
            split_regex_value = '(From[^:].*\n?(?=From:))'
            add_split_val = ''
            #Unzip gzip and decode text
            message_bytes = gzip.decompress(blob.download_as_bytes())
            messages_blob = decode_messsage(message_bytes)
            # Swap all Send reply to or Reply-to with In-Reply-To
            messages_blob = re.sub(r'^^Send reply to:', 'In-Reply-To:', messages_blob)
    except UnicodeDecodeError as err:
        print("Getting GCS data Error: {}. Downloading and decompressing, go over file line by line to resolve.".format(err))
        messages_list_result = decompress_line_by_line(blob, fpath)
    except EOFError as err:
        # Catches a few 'empty' archives for which this error will be generated.
        print('Getting GCS data Error: {}. Not successfully gunzipped or empty and throws err.'.format(err))
    except AttributeError as err:
        # Catches a few 'empty' archives for which this error will be generated.
        print('Getting GCS data Error: {}. Check the file {} exists and spelled correctly.'.format(err, fpath))

    if not messages_list_result and split_regex_value and messages_blob:
        # Split messages into a list through regex to find wanted splits, add unique split value, split on that unique value and filter for any empty values in list. Goal avoid edge cases and retain core info that are part of some split regex value.
        messages_list_result = list(filter(None, re.sub(split_regex_value, split_val+"\\1", messages_blob).split(split_val+add_split_val)))

    return messages_list_result

# TODO apply DLP to PII esp before appending body_bytes - to, from, cc, in-reply-to, message-id, references | email and names and any references that include email addresses
def get_msg_objs_list(msgs, bucketname, filename):
    """Parse the msg texts into a list of header items per msg and pull out body"""
    msg_list = []

    for msg in msgs:
        if msg:  # then parse the message.
            msg_parts = []
            # TODO: is error-handling needed here? It doesn't appear to fail with the current archives.
            res = email.parser.Parser().parsestr(msg)
            msg_parts.extend(res.items())
            msg_parts.append(('mailing_list', bucketname))
            msg_parts.append(('filename', filename))
            msg_parts.extend(parse_body(res))
            msg_list.append(msg_parts)

    return msg_list

# TODO find better way to convert the base64 bytestring to a a string for the json bq ingestion
def encode_body(body):
    """
    Include base64-ified bytestring for the body to bigquery to provide backup in case decoding is corrupted. This may be redundant and if so remove
    """
    # it out)
    if type(body) is str:
        body = body.encode()
    b64_bstring = base64.b64encode(body)
    return (str(b64_bstring)[2:])[:-1]


def parse_body(msg_object):
    """Given a parsed msg object, extract the text version of its body.
    """
    body_objects = []
    body = ""
    # Get body content and add to msg_parts_list
    if msg_object.is_multipart():
        for part in msg_object.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload()
                break
    else:
        body = msg_object.get_payload()

    if body:
        body_objects.append(('Body', body))
        body_objects.append(('body_bytes', encode_body(body)))
    return body_objects

# TODO: Python chat channel, confirmed this approach | alternative if/elif potential but need to confirm will not miss trying all options without throwing one exception that stops it
# TODO investigate how datetime is handled more and see if there is a better way to id and parse different timezone notations. Adjustments may be getting lost.
def parse_datestring(datestring):
    """Given a date string, parse date to the format year-month-dayThour:min:sec and convert to DATETIME-friendly utc time.
    All the different formats are probably due to ancient mail client variants. Older messages have issues.
    """
    datestring = datestring[1]
    date_objects = {}
    date_objects['raw_date_string'] = datestring.strip()

    try:
        formated_date = parser.parse(datestring, tzinfos=timezone_info)
    except (TypeError, parser._parser.ParserError) as err:
        print('Parsing error: {}. For datestring: {}. Trying alternatives.'.format(datestring, err))
        formated_date = datestring.replace('.', ':')
        try:
            if re.search('(.* [-+]\d{4}).*$', datestring):
                pass
            elif re.search('(.* [-+]\d{1,3}).*$', datestring):
                print("Datestring {} was missing full timezone format.".format(datestring))
                ds_list = datestring.split(" ")
                # len should be 5 including the +/- sign
                num_zero_add = 5 - len(ds_list[-1])
                ds_list[-1] = ds_list[-1] + "0"*num_zero_add
                datestring = " ".join(ds_list)
            elif re.search('(.* \d{4}).*$', datestring):
                ds_list = datestring.split(" ")
                if ds_list[-1] == "0000" or ds_list[-1] == "0100":
                    ds_list[-1] = "+" + ds_list[-1]
                datestring = " ".join(ds_list)
            parsed_date = re.search('(.* [-+]\d{4}).*$', datestring)
            formated_date = parser.parse(parsed_date[1])
        except (TypeError, parser._parser.ParserError) as err2:
            print("Tried parse 2: '(.* [-+]\d\d\d\d).*$' and got error: {}".format(err2))
            try:
                parsed_date = re.search('(.*)\(.*\)', datestring)
                formated_date = parser.parse(parsed_date[1])
            except (TypeError, parser._parser.ParserError) as err3:
                print("Tried parse 3: '(.*)\(.*\)' and got error: {}".format(err3))
                try:
                    parsed_date = re.search('(.*) [a-zA-Z]+$', datestring)
                    formated_date = parser.parse(parsed_date[1])
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
    contact_keys = {'from': ['raw_from_string', 'from_name', 'from_email'], 'to': ['raw_to_string','to_name','to_email'], 'author': ['raw_from_string', 'from_name', 'from_email'], 'cc': ['raw_cc_string','cc_name','cc_email'],}
    contact_string = raw_contact

    # Store raw from string after its decoded
    contact_objects[contact_keys[to_from][0]] = contact_string

    # Format from string and replace ' at ' syntax for pipermail email otherwise its ignored
    contact_string = contact_string.lower().strip().replace(' at ', '@')

    # TODO add msg.get_all("from" or "to", []) if there are multiple
    # Split out and store name and email
    parsed_addr = email.utils.getaddresses([contact_string])
    # TODO: better error checks/handling? The raw string will still be stored.
    try:
        if parsed_addr[0][0]:
            contact_objects[contact_keys[to_from][1]] = parsed_addr[0][0]
        if parsed_addr[0][1]:
            contact_objects[contact_keys[to_from][2]] = parsed_addr[0][1]
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
            ee_objects[ee_key] = ee_raw.strip()  # get rid of any leading/trailing whitespace
        except AttributeError as err:
            print('for *{}*, got error {} for {}'.format(ee_key, err, ee_raw))
            ee_objects[ee_key] = '{}'.format(ee_raw.strip())
    # elif ee_key in IGNORED_FIELDS:
    #     print('****Ignoring unsupported message field: {} in msg {}'.format(ee_key, ee_raw))

    # TODO what to do with fields not in ignore or allowed?
        # time.sleep(2)

    return ee_objects

def convert_msg_to_json(msg_objects):
    """takes a list of message objects, and turns them into json dicts for insertion into BQ."""

    json_result = {'refs':[]} # this repeated field is not nullable

    msg_keys = {'date': parse_datestring, 'from': parse_contacts, 'to': parse_contacts, 'author': parse_contacts, 'cc': parse_contacts, 'references': parse_references}

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

# TODO Review this should be deleted
# def format_schema(schema):
#     formatted_schema = []
#     for row in schema:
#         if row["type"] == "RECORD":
#             r_schema = format_schema(row["fields"])
#             formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode'], r_schema))
#         else:
#             formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
#     return formatted_schema

# TODO catch empty content
def store_in_bigquery(client, json_rows, table_id, chunk_size):
    """Insert a list of message dicts into the given BQ table.  chunk_size determines how many
    are loaded at once. (If the payload is too large, it will throw an error.)
    """
    num_rows_loaded = 0
    # Try to get the table and if it doesn't exist, create it using the json format
    try:
        table = client.get_table(table_id)
    except NotFound:
        with open("table_schema.json") as f:
            schema = json.load(f)
        table_framework = bigquery.Table(table_id, schema=schema)
        client.create_table(table_framework)
        table = client.get_table(table_id)

    json_chunks = chunks(json_rows, chunk_size)
    for json_row in json_chunks:
        try:
            errors = client.insert_rows_json(table, json_row)
            if errors:
                print("This json row did not load to BigQuery: {} and threw this error: {}.".format(json_row, errors))
            else:
                num_rows_loaded += chunk_size
                print("{} rows have been added without error.".format(num_rows_loaded))
        except BadRequest as err:
            print("{} error thrown loading json_row to BigQuery. Trying to load again with reduced chunk size.".format(err))
            reduce_chunk_size = int(chunk_size/2)
            num_rows_loaded += store_in_bigquery(client, json_row, table_id, reduce_chunk_size)
    return num_rows_loaded

def get_bucket_name(bucket_name):
    if '-gzip' in bucket_name:
        return bucket_name.replace('-gzip', '')
    elif '-txt' in bucket_name:
        return bucket_name.replace('-txt', '')
    return bucket_name

def get_filenames(storage_client, bucketname, filenames=None, prefix=None):
    # Get list of filenames
    if filenames:  # for testing: process just this file
        return filenames.split(" ")
    return list_bucket_filenames(storage_client, bucketname, prefix)

def get_table_id(projectid, tableid):
    return "{}.{}".format(projectid, tableid)

def main():
    argparser = argparse.ArgumentParser(description='BQ message ingestion')
    argparser.add_argument('--bucketname', help='GCS bucketname to pull data from', required=True)
    argparser.add_argument('--tableid', help="Required BigQuery table id to store files into in the format `your-project.your_dataset.your_table`", required=True)
    argparser.add_argument('--filename', help='Optional, pass in single filename esp for testing or multiple. If left off then it pulls all filenames in bucket')
    # TODO use prefix to pull smaller groups of files if needed esp when running monthly
    argparser.add_argument('--prefix', default=None, help='Optional, to filter subdirectories and filenames based on prefix.')
    argparser.add_argument('--chunk_size', type=int, help='How many rows to load into BigQuery at a time.', default=200)
    argparser.add_argument('--ingest', default=True, help='Run the ingestion to BQ', action='store_true')
    argparser.add_argument('--no-ingest', dest='ingest', help='Do not run the ingestion to BQ. Esp for testing', action='store_false')
    argparser.add_argument('--projectid', help='Project id')

    args, unknown = argparser.parse_known_args()
    if unknown:
        print("Unknown argparser args: ", unknown)

    tableid = get_table_id(args.projectid, args.tableid)
    print('----using table: {}----'.format(tableid))

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

# TODO pull all below into script to test - note error if there is a space inn the filename that is entered - it is empty and creates an error on blob above
    filenames = get_filenames(storage_client, args.bucketname, args.filename, args.prefix)
    bucket_name = get_bucket_name(args.bucketname)

    for filename in filenames:
        print('---------------')
        print('Working on file: {} from bucket: {}'.format(filename, bucket_name))

        msgs_list = get_msgs_from_gcs(storage_client, args.bucketname, filename)

        if msgs_list:
            msg_obj_list = get_msg_objs_list(msgs_list, bucket_name, filename)
            json_result = []
            # Create list of json dicts from parsed msg info
            result = list(map(convert_msg_to_json,msg_obj_list))
            json_result.extend(result)

            if args.ingest:
                store_in_bigquery(bigquery_client, json_result, tableid, args.chunk_size)
                # time.sleep(1)

        else:
            print('*****No msgs obtained for {}'.format(filename))
            # time.sleep(5)


if __name__ == "__main__":
    main()
