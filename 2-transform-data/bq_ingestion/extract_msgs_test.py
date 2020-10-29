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


import unittest
import datetime
from dateutil import parser
import extract_msgs as em
import email
import mock
import gzip

class Test(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)
        self.ex_binary_post_from_uk = b''.join([
            b'From: UK Parliment <uk.parliment@gmail.com>\n',
            b'To: Emmeline Pankhurst <emmeline.pankhurst@gmail.com>\n',
            b'Subject: Voting Rights\n',
            b'Date: Mon, July 2 1928 13:46:03 +0100\n',
            b'MIME-Version: 1.0\n',
            b'Content-Type: text/plain; charset="utf-8"\n',
            b'Content-Transfer-Encoding: 7bit\n',
            b'References: <voting-rights-id@mail.gmail.com>\n',
            b'Message-ID: <voting-rights-id@mail.gmail.com>\n',
            b'\n',
            b'Full women voting rights passed in U.K.\n',
            b'\n',
            b'"We are here, not because we are law-breakers; we are here in our efforts to become law-makers."\n'],)
        self.ex_text_post_from_uk = 'From UK Parliment <uk.parliment@gmail.com> Mon July 2  13:46:03 1928\nFrom: UK Parliment <uk.parliment@gmail.com>\nTo: Emmeline Pankhurst <emmeline.pankhurst@gmail.com>\nSubject: Voting Rights\nDate: Mon, July 2 1928 13:46:03 +0100\nMIME-Version: 1.0\nContent-Type: text/plain; charset="utf-8"\nContent-Transfer-Encoding: 7bit\nReferences: <voting-rights-id@mail.gmail.com>\nMessage-ID: <voting-rights-id@mail.gmail.com>\n\nFull women voting rights passed in U.K.\n\n"We are here, not because we are law-breakers; we are here in our efforts to become law-makers."\n'
        self.ex_text_post_from_us1 = 'From us.congress at gmail.com Wed Aug 18  11:00:07 1920\nFrom: us.congress at gmail.com (US Congress)\nTo: staton.anthony@gmail.com\nSubject: 19th Ammendment\nDate: Wed, Aug 18 1920 11:00:07 +0100\nMessage-ID:<19th-ammendment-id@mail.gmail.com>\nMIME-Version: 1.0\nContent-Type: text/plain; charset="utf-8"\nContent-Transfer-Encoding: 7bit\nReferences: <19th-ammendment-id@mail.gmail.com>\n19th Amemndment ratified in U.S. granting women the right to vote after the final vote in Tennessee.\n\nAs per the Declaration of Sentiments in 1848, "We hold these truths to be self-evident: that all men and women are created equal; that they are endowed by their Creator with certain inalienable rights; that among these are life, liberty, and the pursuit of happiness."\n'
        self.ex_text_post_from_us2 ='From US Congress <us.congress@gmail.com> Wed Aug 6 15:32:20 1965 \nFrom: us.congress at gmail.com (US Congress)\nTo: ida.b.wells@gmail.com\nSubject: Voter Rights Act\nDate: Wed, Aug 6 1965 15:32:20 +0100\nMessage-ID:<voter-rights-act-id@mail.gmail.com>\nMIME-Version: 1.0\nContent-Type: text/plain; charset="utf-8"\nContent-Transfer-Encoding: 7bit\nReferences: <voter-rights-act-id@mail.gmail.com>\nVoter`s Rights Act outlawed discriminatory voting practices.\n\nFrom 1913 suffrage march in DC, "Either I go with you or not at all. I am not taking this stand because I personally wish for recognition. I am doing it for the future benefit of my whole race."\n'
        self.ex_text_post_split_n_us1='/nX-Received: by 65.19.006.002 with SMTP id 19651965vote.65.555555555555;\nMIME-Version: 1.0\nSender: us.congress@gmail.com\nReceived: by 10.90.70.10 with HTTP; Wed, Aug 18 1920 11:00:07 -0700 (PDT)\nIn-Reply-To:<voting-rights-id@mail.gmail.com>\nReferences: <19th-ammendment-id@mail.gmail.com>\nFrom: US Congress <us.congress@gmail.com>\nDate: Wed, Aug 18 1920 11:00:07 +0100\nMessage-ID:<19th-ammendment-id@mail.gmail.com>\nSubject: 19th Ammendment\nTo: staton.anthony@gmail.com\nContent-Type: text/plain; charset=ISO-8859-1\nContent-Transfer-Encoding: quoted-printable\n\n19th Amemndment ratified in U.S. granting women the right to vote after the final vote in Tennessee.\n\nAs per the Declaration of Sentiments in 1848, "We hold these truths to be self-evident: that all men and women are created equal; that they are endowed by their Creator with certain inalienable rights; that among these are life, liberty, and the pursuit of happiness."\n'
        self.ex_text_post_split_n_us2_author_cc='/nMIME-Version: 1.0\nDate: Wed, Aug 6 1965 15:32:20 +0100\nFrom: voter@us.com\nAuthor: US Congress <us.congress@gmail.com>\nTo: ida.b.wells@gmail.com\nCC: Frances Ellen Watkins Harper \nMessage-ID:<voter-rights-act-id@mail.gmail.com>\nSubject: Voter Rights Act\nContent-Type: text/plain; charset="utf-8"\nContent-Transfer-Encoding: 7bit\nReferences: <voter-rights-act-id@mail.gmail.com><ida.b.wells@gmail.com>\n\nVoter`s Rights Act outlawed discriminatory voting practices.\n\nFrom 1913 suffrage march in DC, "Either I go with you or not at all. I am not taking this stand because I personally wish for recognition. I am doing it for the future benefit of my whole race."\n'
        self.ex_html = 'Voter Rights <html> for all women</html> in 1965'
        self.ex_html_removed = 'Voter Rights  in 1965'
        self.ex_message_edgecases_txt = '''Content-Transfer-Encoding: base64dOx5MG/nE5MG/1tD
        6Gti/nX-BeenThere:golang-nuts@googlegroups.com 
        Received: by 10.87.38.4 with SMTP id 3.p; Mon, 23 Nov 2009
        http://naacp/naacp.html
        //Join the 
        NAACP started in 1909
        and pursue civil rights
        = )/nReceived: by 10.91.72.12 with SMTP id'''
        self.ex_message_split_edgecases_txt = ['Content-Transfer-Encoding: base64dOx5MG/nE5MG/1tD\n        6Gti',
                                           'X-BeenThere:golang-nuts@googlegroups.com \n'
                                           '        Received: by 10.87.38.4 with SMTP id 3.p; Mon, 23 Nov 2009\n'
                                           '        http://naacp/naacp.html\n'
                                           '        //Join the \n'
                                           '        NAACP started in 1909\n'
                                           '        and pursue civil rights\n'
                                           '        = )',
                                           'Received: by 10.91.72.12 with SMTP id']
        self.ex_message_edgecases_gzip = '''From: Frances Ellen Watkins Harper
        From: Mary Church Terrell
        From: Nannie Helen Burroughs'''
        self.ex_message_split_edgecases_gzip =['From: Frances Ellen Watkins Harper\n'
                                               '        From: Mary Church Terrell\n'
                                               '        From: Nannie Helen Burroughs']
        self.ex_parsed_msg_single = [[('From', 'UK Parliment <uk.parliment@gmail.com>'),
                                      ('To', 'Emmeline Pankhurst <emmeline.pankhurst@gmail.com>'),
                                      ('Subject', 'Voting Rights'),
                                      ('Date', 'Mon, July 2 1928 13:46:03 +0100'),
                                      ('MIME-Version', '1.0'),
                                      ('Content-Type', 'text/plain; charset="utf-8"'),
                                      ('Content-Transfer-Encoding', '7bit'),
                                      ('References', '<voting-rights-id@mail.gmail.com>'),
                                      ('Message-ID','<voting-rights-id@mail.gmail.com>'),
                                      ("mailing_list", "pankhurst-bucket"),
                                      ('filename', '1999-04.mbox.gzip'),
                                      ('Body','Full women voting rights passed in U.K.\n\n"We are here, not because we are law-breakers; we are here in our efforts to become law-makers."\n')]]
        self.ex_parsed_msg_single_auth_cc =[[('From', 'UK Parliment <uk.parliment@gmail.com>'),
                                            ('To', 'ida.b.wells@gmail.com'),
                                            ('Subject', 'Voter Rights Act'),
                                            ('Date', 'Wed, Aug 6 1965 15:32:20 +0100'),
                                            ('Author', 'US Congress <us.congress@gmail.com>'),
                                            ('CC', 'Emmeline Pankhurst <emmeline.pankhurst@gmail.com>'),
                                            ('Message-ID', '<voter-rights-act-id@mail.gmail.com>'),
                                            ('MIME-Version', '1.0'),
                                            ('Content-Typ', 'text/plain; charset="utf-8"'),
                                            ('Content-Transfer-Encoding', '7bit'),
                                            ('References:' ,'<voter-rights-act-id@mail.gmail.com>'),
                                            ('mailing_list', 'voter-bucket'),
                                            ('filename', '1999-04.mbox.gzip'),
                                            ('Body', '\nVoter`s Rights Act outlawed discriminatory voting practices.\n\nFrom 1913 suffrage march in DC, "Either I go with you or not at all. I am not taking this stand because I personally wish for recognition. I am doing it for the future benefit of my whole race."\n')]]
        self.ex_parsed_msg_mult = [[('From', 'us.congress at gmail.com (US Congress)'),
                                    ('To', 'staton.anthony@gmail.com'),
                                    ('Subject', '19th Ammendment'),
                                    ('Date', 'Wed, Aug 18 1920 11:00:07 +0100'),
                                    ('Message-ID', '<19th-ammendment-id@mail.gmail.com>'),
                                    ('MIME-Version', '1.0'),
                                    ('Content-Type', 'text/plain; charset="utf-8"'),
                                    ('Content-Transfer-Encoding', '7bit'),
                                    ('References', '<19th-ammendment-id@mail.gmail.com>'),
                                    ('mailing_list', 'voter-bucket'),
                                    ('filename', '1999-04.txt'),
                                    ('Body', '19th Amemndment ratified in U.S. granting women the right to vote after the final vote in Tennessee.\n\nAs per the Declaration of Sentiments in 1848, "We hold these truths to be self-evident: that all men and women are created equal; that they are endowed by their Creator with certain inalienable rights; that among these are life, liberty, and the pursuit of happiness."\n')],
                                   [('From', 'us.congress at gmail.com (US Congress)'),
                                    ('To', 'ida.b.wells@gmail.com'),
                                    ('Subject', 'Voter Rights Act'),
                                    ('Date', 'Wed, Aug 6 1965 15:32:20 +0100'),
                                    ('Message-ID', '<voter-rights-act-id@mail.gmail.com>'),
                                    ('MIME-Version', '1.0'),
                                    ('Content-Type', 'text/plain; charset="utf-8"'),
                                    ('Content-Transfer-Encoding', '7bit'),
                                    ('References', '<voter-rights-act-id@mail.gmail.com>'),
                                    ('mailing_list', 'voter-bucket'),
                                    ('filename', '1999-04.txt'),
                                    ('Body', 'Voter`s Rights Act outlawed discriminatory voting practices.\n\nFrom 1913 suffrage march in DC, "Either I go with you or not at all. I am not taking this stand because I personally wish for recognition. I am doing it for the future benefit of my whole race."\n')]]

    # TODO is iso8859 really being tested?
    def test_decode_messsage(self):

        encoded_input = {
            "test1": {
                "comparison_type": "Decode utf8 with from and date",
                "data": b'From ida.b.wells@gmail.com Tue Sep  1 04:14:32 2020\n'
            },
            "test2": {
                "comparison_type": "Decode utf8 with text and special symbols",
                "data": b"Women's Voting Rights.\n"},
            "test3": {"comparison_type":"Decode iso-8859-1",
                      "data": b'\xe0'},
            "test4": {
                "comparison_type":"Decode iso-8859-2",
                "data": b'hello ab\xe4c\xf6'
            },
        }
        want_decode = {
            "test1": 'From ida.b.wells@gmail.com Tue Sep  1 04:14:32 2020\n',
            "test2": "Women's Voting Rights.\n",
            "test3": 'à',
            "test4": 'hello abäcö',
        }

        for key, test in encoded_input.items():
            # print(test['comparison_type'])
            got_decode = em.decode_messsage(test["data"])
            self.assertEqual(want_decode[key], got_decode, "Decode message error")

        #Test passing string error
        test5_input = "Hello New York"
        want_test5 = AttributeError
        self.assertRaises(want_test5, em.decode_messsage, test5_input, "Error raising AttributeError in decode test.")

    # TODO setup test
    def test_decompress_line_by_line(self):
        pass

    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.bucket.Bucket")
    @mock.patch("google.cloud.storage.blob.Blob")
    def create_bucket_mock(self, bucket_name, blob_name, content_type, blob_contents,  Blob, Bucket, Client):
        client = Client()

        bucket_mock = Bucket(name=bucket_name)
        blob_mock = Blob(name=blob_name)

        client.get_bucket.return_value = bucket_mock
        bucket_mock.get_blob.return_value = blob_mock

        blob_mock.content_type = content_type
        if 'text/plain' in content_type:
            blob_mock.download_as_text.return_value = blob_contents
        elif 'application/x-gzip' in content_type:
            blob_mock.download_as_bytes.return_value = blob_contents

        return client

    # TODO test for when decode_by_line is triggered and test for errors
    def test_get_msgs_from_gcs(self):

        input_gcs = {
            "test1": {
                "comparison_type": "Test single gzip message example with From as the split value",
                "client": self.create_bucket_mock('pankhurst-bucket', 'uk-rights-blob', 'application/x-gzip', gzip.compress(self.ex_text_post_from_uk.encode())),
                "bucket_name": 'pankhurst-bucket',
                "filename":'1918-07.txt.gz'
            },
            "test2": {
                "comparison_type": "Test multiple gzip message example with From as the split value",
                "client": self.create_bucket_mock('wells-bucket', 'us-full-rights-blob', 'application/x-gzip', gzip.compress(self.ex_text_post_from_us1.encode()+self.ex_text_post_from_us2.encode())),
                "bucket_name": 'wells-bucket',
                "filename": '1920-08.mbox.gz'
            },
            "test3": {

                "comparison_type": "Test single text message example with /n as the split value",
                "client": self.create_bucket_mock('stanton-anthony-bucket', 'us-rights-blob', 'text/plain', self.ex_text_post_split_n_us1),
                "bucket_name": 'stanton-anthony-bucket',
                "filename": '1920-08.txt'

            },
            "test4": {
                "comparison_type": "Test multiple text message example with /n as the split value",
                "client": self.create_bucket_mock('stanton-anthony-bucket', 'us-rights-blob', 'text/plain', self.ex_text_post_split_n_us1 + self.ex_text_post_split_n_us1),
                "bucket_name": 'stanton-anthony-bucket',
                "filename": '1920-08.txt'

            },
            "test5": {
                "comparison_type": "Test single text message example with /n as the split value and includes CC and Author",
                "client": self.create_bucket_mock('wells-bucket', 'us-full-rights-blob', 'text/plain', self.ex_text_post_split_n_us2_author_cc),
                "bucket_name": 'wells-bucket',
                "filename": '1965-08.txt'
            },
            "test6": {
                "comparison_type": "Test multiple text message example with /n as the split value and includes CC and Author",
                "client": self.create_bucket_mock('wells-bucket', 'us-full-rights-blob', 'text/plain', self.ex_text_post_split_n_us2_author_cc+self.ex_text_post_split_n_us2_author_cc),
                "bucket_name": 'wells-bucket',
                "filename": '1909-02.txt'
            },
            "test7": {
                "comparison_type": "Test edge cases for text split on /n",
                "client": self.create_bucket_mock('naacp-bucket', 'naacp-blob', 'text/plain', self.ex_message_edgecases_txt),
                "bucket_name": 'naacp-bucket',
                "filename": '1965-08.txt'
            },
            "test8": {
                "comparison_type": "Test edge cases for gzip split on /n",
                "client": self.create_bucket_mock('suffragist-bucket', 'suffragist-blob', 'application/x-gzip', gzip.compress(self.ex_message_edgecases_gzip.encode())),
                "bucket_name": 'suffragist-bucket',
                "filename": '1896-08.txt'
            },
            "test9": {
                "comparison_type": "Test change 'Send reply to'",
                "client": self.create_bucket_mock('voter-rights-bucket', 'vr-blob', 'application/x-gzip', gzip.compress(b'Send reply to:<voting-rights@mail.gmail.com>\n')),
                "bucket_name": 'voter-rights-bucket',
                "filename": '1965-08.txt.gz'
            },

        }
        want_msg_list = {
            "test1": [self.ex_text_post_from_uk],
            "test2": [self.ex_text_post_from_us1,self.ex_text_post_from_us2],
            "test3": [self.ex_text_post_split_n_us1[2:]],
            "test4": [self.ex_text_post_split_n_us1[2:], self.ex_text_post_split_n_us1[2:]],
            "test5": [self.ex_text_post_split_n_us2_author_cc[2:]],
            "test6": [self.ex_text_post_split_n_us2_author_cc[2:], self.ex_text_post_split_n_us2_author_cc[2:]],
            "test7": self.ex_message_split_edgecases_txt,
            "test8": self.ex_message_split_edgecases_gzip,
            "test9": ["In-Reply-To:<voting-rights@mail.gmail.com>\n"]
        }

        for key, test in input_gcs.items():
            # print(test['comparison_type'])
            got_msg_list= em.get_msgs_from_gcs(test['client'], test['bucket_name'], test['filename'])
            self.assertEqual(want_msg_list[key], got_msg_list, "Get msg from gcs error")

    def test_get_msg_objs_list(self):

        msg_input = {
            "test1": {
                "comparison_type": "Test getting message parts from single message",
                "msgs": [self.ex_text_post_from_uk],
                "bucketname":"pankhurst-bucket",
                "filename":"1999-04.mbox.gzip"
            },
            "test2": {
                "comparison_type": "Test getting message parts from multiple messages",
                "msgs":[self.ex_text_post_from_us1, self.ex_text_post_from_us2],
                "bucketname": "voter-bucket",
                "filename":"1999-04.txt"
            },
        }
        want_msg_list = {
            "test1": self.ex_parsed_msg_single,
            "test2": self.ex_parsed_msg_mult,
        }
        #
        # TODO mock getting the body and skip that call?
        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_msg_list = em.get_msg_objs_list(test["msgs"], test["bucketname"], test["filename"])
            for msg in got_msg_list:
                for indx, (msg_key, _) in enumerate(msg):
                    if msg_key == 'body_bytes':
                        msg.pop(indx)
                        break
                self.assertEqual(msg_key, 'body_bytes', "Body bytes missing get msg objects")
            # Remove body_bytes content from comparison
            self.assertEqual(want_msg_list[key], got_msg_list, "Get msg objects error")


    def test_parse_body(self):

        msg_input = {
            "test1": {
                "comparison_type": "Test getting body from multipart message",
                "msg_obj": email.message_from_string(self.ex_text_post_from_uk)
            },
            "test2": {
                "comparison_type": "Test getting body from single part message",
                    "msg_obj": email.message_from_string('What is the Voter Rights Act?\n'),
            }
        }
        want_msg_list = {
            "test1": [('Body', 'Full women voting rights passed in U.K.\n\n"We are here, not because we are law-breakers; we are here in our efforts to become law-makers."\n')],
            "test2": [('Body', 'What is the Voter Rights Act?\n')],
        }
        #
        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_msg_list = em.parse_body(test["msg_obj"])
            self.assertEqual(got_msg_list[-1][0], 'body_bytes', "Body bytes missing in test: " + test['comparison_type'])
            # Remove body_bytes content from comparison
            self.assertEqual(want_msg_list[key], got_msg_list[:-1], "Parse body error")

    # TODO test empty date and all the exceptions
    def test_parse_datestring(self):

        date_input = {
            "test1": {
                "comparison_type": "Test standard date format w/ 1 dig date and neg 8 hr GMT offset",
                "input": ("Date", "Sat, 6 Aug 1965 22:11:18 -0800")
            },
            "test2": {
            "comparison_type": "Test day month format w/o week day and w/ 2 dig date and pos 2 hr GMT offset",
            "input": ("", "15 Oct 2000 19:52:16 +0200"),
            },
            "test4": {
                "comparison_type":"Test standard date format w/ 2 dig date and pos 1 hr GMT offset and timezone note",
                "input":("Date", "Tue, 13 Feb 2001 08:17:03 +0100 (MET)")},
            "test5": {
                "comparison_type": "Test day month format w/o week day w/ 1 dig date and 8 hr GMT offset",
                "input":("Date", "6 Nov 2006 11:11:19 -0800")},
            "test6": {
                "comparison_type": "Test standard date format and timezone letters but no digit offset",
                "input": ("Date", "Wed, 25 Oct 2006 19:21:24 GMT")},
            "test7": {
                "comparison_type": "Test day month format w/o week day w/ 2 dig date and 8 hr GMT offset and timezone note",
                "input": ("Date", "25 May 2006 03:11:24 GMT") },
            "test9": {
                "comparison_type": "Test missing digets on time offset",
                "input": ("Date", "Sun, 05 Nov 2000 19:04:06 -050")},
            "test10": {
                "comparison_type": "Test with time offset included daylight savings time notation DST or other standard time formats like PST, CEST...",
                "input": ("Date", "Fri, 26 May 2000 09:17:50 +0200 (MET DST)")},
            "test11": {
                "comparison_type": "Test timezone wihtout +-",
                "input": ("Date", "Sun, 05 Nov 2000 19:04:06  0000")},
            "test12": {
                "comparison_type": "Test timezone spelled out",
                "input": ("Date", "Fri, 8 Dec 2000 09:37:24 -0800 (Pacific Standard Time)")},
            "test13": {
                "comparison_type": "Test timezone wihtout timezone with offset in ()",
                "input": ("Date", "Fri, 15 Dec 2000 16:53:48 +0200 (GMT-2)")},
            "test14": {
                "comparison_type": "Test timezone that requires tzinfos passed into parse otherwise throws UnknownTimezoneWarning",
                "input": ("Date", "Sat, 1 Apr 2000 12:00:00 -0500 CDT")},
        }
        want_date = {
            "test1": {'date': '1965-08-07 06:11:18',
                    'raw_date_string': 'Sat, 6 Aug 1965 22:11:18 -0800'},
            "test2": {'date': '2000-10-15 17:52:16', 'raw_date_string': '15 Oct 2000 19:52:16 +0200'},
            "test4": {'date': '2001-02-13 07:17:03',
                  'raw_date_string': 'Tue, 13 Feb 2001 08:17:03 +0100 (MET)'},
            "test5": {'date': '2006-11-06 19:11:19', 'raw_date_string': '6 Nov 2006 11:11:19 -0800'},
            "test6": {'date': '2006-10-25 19:21:24',
                    'raw_date_string': 'Wed, 25 Oct 2006 19:21:24 GMT'},
            "test7": {'date': '2006-05-25 03:11:24', 'raw_date_string': '25 May 2006 03:11:24 GMT'},
            "test9": {'date': '2000-11-06 00:04:06', 'raw_date_string': 'Sun, 05 Nov 2000 19:04:06 -050'},
            "test10": {'date': '2000-05-26 07:17:50', 'raw_date_string': 'Fri, 26 May 2000 09:17:50 +0200 (MET DST)'},
            "test11": {'date': '2000-11-05 19:04:06', 'raw_date_string': 'Sun, 05 Nov 2000 19:04:06  0000'},
            "test12": {'date': '2000-12-08 17:37:24', 'raw_date_string': 'Fri, 8 Dec 2000 09:37:24 -0800 (Pacific Standard Time)'},
            "test13": {'date': '2000-12-15 14:53:48', 'raw_date_string': 'Fri, 15 Dec 2000 16:53:48 +0200 (GMT-2)'},
            "test14": {'date': '2000-04-01 17:00:00', 'raw_date_string': 'Sat, 1 Apr 2000 12:00:00 -0500 CDT'}
        }

        for key, test in date_input.items():
            # print(test['comparison_type'])
            got_date = em.parse_datestring(test["input"])
            self.assertEqual(want_date[key], got_date, "Parse datestring error got.")

    def test_parse_contacts(self):
        msg_input = {
            "test1": {
                "comparison_type": "Test get from contact from string",
                "msg_obj": ('From', "US Congress <us.congress@gmail.com>\n")
            },
            "test2": {
                "comparison_type": "Test get to contact from string",
                "msg_obj": ('To',"Ida B Wells <ida.b.wells@gmail.com>\n")
            },
            "test3": {
                "comparison_type": "Test get contact from string without <> around email",
                "msg_obj": ('To',"ida.b.wells@gmail.com\n")
            },
            "test4": {
                "comparison_type": "Test get contact from string without name and with <>",
                "msg_obj": ('From', "<us.congress@gmail.com>\n")
            },
            "test5": {
                "comparison_type": "Test get contact author string",
                "msg_obj": ('Author', "US Congress <us.congress@gmail.com>\n")
            },
            "test6": {
                "comparison_type": "Test get contact cc string",
                "msg_obj": ('CC',"Ida B Wells <ida.b.wells@gmail.com>\n")
            },
            "test7": {
                "comparison_type": "Test get from contact from string with at changed to @",
                "msg_obj": ('From', 'From: us.congress at gmail.com (US Congress)\n')
            },

            # TODO email utils does not parse the name - potentially need alternative
            # "test": {
            #     "comparison_type": "Test get contact without email",
            #     "msg_obj": ('To',"Ida B Wells\n")
            # },

        }
        want_msg_list = {
            "test1": {'raw_from_string': 'US Congress <us.congress@gmail.com>\n', 'from_name': "us congress", 'from_email': 'us.congress@gmail.com' },
            "test2": {'raw_to_string': 'Ida B Wells <ida.b.wells@gmail.com>\n', 'to_name': "ida b wells", 'to_email': 'ida.b.wells@gmail.com'},
            "test3": {'raw_to_string': 'ida.b.wells@gmail.com\n','to_email': 'ida.b.wells@gmail.com'},
            "test4": {'raw_from_string': '<us.congress@gmail.com>\n', 'from_email': 'us.congress@gmail.com' },
            "test5": {'raw_from_string': 'US Congress <us.congress@gmail.com>\n', 'from_name': "us congress", 'from_email': 'us.congress@gmail.com' },
            "test6": {'raw_cc_string': 'Ida B Wells <ida.b.wells@gmail.com>\n', 'cc_name': "ida b wells", 'cc_email': 'ida.b.wells@gmail.com'},
            "test7": {'raw_from_string': 'From: us.congress at gmail.com (US Congress)\n', 'from_name': "us congress", 'from_email': 'us.congress@gmail.com' },
            # "test": {'raw_to_string': 'Ida B Wells\n', 'to_name': "ida b wells"},
        }
        #
        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_msg_list = em.parse_contacts(test["msg_obj"])
            self.assertEqual(want_msg_list[key], got_msg_list, "Parse contacts error")

    def test_parse_references(self):
        msg_input = {
            "test1": {
                "comparison_type": "Test get reference from string",
                "msg_obj": ('References', '<voting-rights-id@mail.gmail.com>'),
            },
            "test2": {
                "comparison_type": "Test get multiple references from string",
                "msg_obj": ('References', '<voting-rights-id@mail.gmail.com> <ida.b.wells@gmail.com>'),
            },
        }
        want_msg_list = {
            "test1": {'raw_refs_string': '<voting-rights-id@mail.gmail.com>', 'refs': [{'ref':'<voting-rights-id@mail.gmail.com>'}] },
            "test2": {'raw_refs_string': '<voting-rights-id@mail.gmail.com> <ida.b.wells@gmail.com>', 'refs': [{'ref':'<voting-rights-id@mail.gmail.com>'},{'ref': '<ida.b.wells@gmail.com>'}]},
        }
        #
        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_msg_list = em.parse_references(test["msg_obj"])
            self.assertEqual(want_msg_list[key], got_msg_list, "Parse references error")

# TODO test all pairs - not everything covered
    def test_parse_everything_else(self):
        msg_input = {
            "test1": {
                "comparison_type": "Test parse message id from string",
                "msg_obj":('Message-ID', '\n <voting-rights-id@mail.gmail.com>'),
            },
            "test2": {
                "comparison_type": "Test parse MIME version from string and get nothing because ignored",
                "msg_obj": ('MIME-Version', '1.0'),
            },
            "test3": {
                "comparison_type": "Test parse content type from string and get nothing because ignored",
                "msg_obj": ('Content-Type', 'text/plain; charset="utf-8"'),
            },
            "test4": {
                "comparison_type": "Test content transfer encoding from string and get nothing because ignored",
                "msg_obj": ('Content-Transfer-Encoding', '7bit'),
            },
            "test5": {
                "comparison_type": "Test parse subjectfrom string",
                "msg_obj": ('Subject', '19th Ammendment'),
            },
            "test6": {
                "comparison_type": "Test parse in replyt to type from string",
                "msg_obj": ('In-Reply-To', '<voting-rights-id@mail.gmail.com>'),
            },
        }

        want_msg_list = {
            "test1": {'message_id': '<voting-rights-id@mail.gmail.com>' },
            "test2": {},
            "test3": {'content_type': 'text/plain; charset="utf-8"'},
            "test4": {},
            "test5": {'subject': '19th Ammendment'},
            "test6": {'in_reply_to': '<voting-rights-id@mail.gmail.com>'},
        }

        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_msg_list = em.parse_everything_else(test["msg_obj"])
            self.assertEqual(want_msg_list[key], got_msg_list, "Parse everything else error")


    # TODO test one email address, multiple, with or with or without names, with or without symbols
    # TODO test references where there is one, none and multiple provided
    # Note the email and other PPI content should be DLPed
    def test_convert_msg_to_json(self):

        msg_input = {
            "test1": {
                "comparison_type":"Test processing msg list",
                "msg": self.ex_parsed_msg_single[0]
            },
            "test2": {
                "comparison_type":"Test processing msg list with author replacing from and cc is captured",
                "msg": self.ex_parsed_msg_single_auth_cc[0]
            },
            "test3": {
                "comparison_type":"Test skips if object value doesn't exist",
                "msg": [('To', 'ida.b.wells@gmail.com'),
                         ('Subject', 'Voter Rights Act'),
                         ('Date', ''),
                         ('CC', ''),
                        ('mailing_list', 'voter-bucket'),
                        ('filename', '1999-04.mbox.gzip')]
            }
        }
        want_json = {
            "test1": {'refs': [{'ref': '<voting-rights-id@mail.gmail.com>'}],
                    'raw_from_string': 'UK Parliment <uk.parliment@gmail.com>',
                    'from_name': 'uk parliment',
                    'from_email': 'uk.parliment@gmail.com',
                    'raw_to_string': 'Emmeline Pankhurst <emmeline.pankhurst@gmail.com>',
                    'to_name': 'emmeline pankhurst',
                    'to_email': 'emmeline.pankhurst@gmail.com',
                    'subject': 'Voting Rights',
                    'raw_date_string': 'Mon, July 2 1928 13:46:03 +0100',
                    'date': '1928-07-02 12:46:03',
                    'content_type': 'text/plain; charset="utf-8"',
                    'message_id': '<voting-rights-id@mail.gmail.com>',
                    'body': 'Full women voting rights passed in U.K.\n\n"We are here, not because we are law-breakers; we are here in our efforts to become law-makers."',
                    'raw_refs_string': '<voting-rights-id@mail.gmail.com>',
                    'mailing_list': 'pankhurst-bucket',
                      'filename': '1999-04.mbox.gzip'
                    },
            "test2": {'refs': [],
                      'raw_from_string': 'US Congress <us.congress@gmail.com>',
                      'from_name': 'us congress',
                      'from_email': 'us.congress@gmail.com',
                      'raw_to_string': 'ida.b.wells@gmail.com',
                      'to_email': 'ida.b.wells@gmail.com',
                      'raw_cc_string': 'Emmeline Pankhurst <emmeline.pankhurst@gmail.com>',
                      'cc_name': 'emmeline pankhurst',
                      'cc_email': 'emmeline.pankhurst@gmail.com',
                      'subject': 'Voter Rights Act',
                      'raw_date_string': 'Wed, Aug 6 1965 15:32:20 +0100',
                      'date': '1965-08-06 14:32:20',
                      'message_id': '<voter-rights-act-id@mail.gmail.com>',
                      'body': 'Voter`s Rights Act outlawed discriminatory voting practices.\n\nFrom 1913 suffrage march in DC, "Either I go with you or not at all. I am not taking this stand because I personally wish for recognition. I am doing it for the future benefit of my whole race."',
                      'mailing_list': 'voter-bucket',
                      'filename': '1999-04.mbox.gzip',},
            "test3":{'refs': [],
                     'raw_to_string': 'ida.b.wells@gmail.com',
                     'to_email': 'ida.b.wells@gmail.com',
                     'subject': 'Voter Rights Act',
                     'mailing_list': 'voter-bucket',
                     'filename': '1999-04.mbox.gzip'
            }
        }
        #
        for key, test in msg_input.items():
            # print(test['comparison_type'])
            got_json = em.convert_msg_to_json(test["msg"])
            self.assertEqual(want_json[key], got_json, "Convert message to json error")

    # TODO simulate load to BQ and test the components of this function esp errors
    def test_store_in_bigquery(self):
        pass

if __name__ == '__main__':
    unittest.main()