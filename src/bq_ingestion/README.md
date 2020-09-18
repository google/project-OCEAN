# Mail archive ingestion and processing

(In progress. Scripts work but need cleanup and are not yet ready for review).

## ingestion script

The [`extract_msgs.py`][1] script reads the `pipermail` and `mailman` archives, parses the message info, and inserts the info into a bigquery table. The script assumes the schema here: [`table_schema.json`][2].

Some known issues/glitches, not all of which are probably worth fixing.
**Text encodings:**
- for the pipermail archives, it appears that the body text is ascii encoding, with non-ascii chars as literal `?` chars. This is not an issue with the mailman archives. (The header lines have encoding info; it’s just the body).
- In some cases, the wrong text encoding is inferred (I’m using the `chardet` package when encoding is not provided).  However, there doesn’t seem to be any way to address  this that works in all cases. 

**Extracting info from the mail messages:**
- In the earlier years of the archives, the date format used by the mail clients is quite variable, and while the script supports a series of ways to try to parse a date string, in rare cases it does not succeed.  In all cases I’m preserving the `raw_date_string` in the BQ table as well.
- Similarly, in some rare cases— particularly with the pipermail archives— the email address and name are not successfully extracted from the `From` string, in cases where there are missing close brackets, etc. The `raw_date_string` field in the BQ table holds the original.
- For multipart messages, I’m currently only extracting and retaining the `text/plain` version. 

### About the BQ table schema

The table schema is here: [`table_schema.json`][3].
Some things to note:
- the `references` header is both stored as a string, and as a ‘repeated record’, where each ref is parsed out individually. (Note: the mailman archives don’t seem to have a ‘references’ field).
- See the notes above regarding storage of the raw ‘date’ and ‘from’ strings as well as their parsed info.
- The source archive is preserved in the `list` field.
- The `body_bytes` field is storing the bytestring of the body text.  This is probably redundant and not really needed.  (To decode and read it, you can use this function in the BQ sql: `safe_convert_bytes_to_string(body_bytes)`).

(more TBD).

#### Schema & table considerations
Some questions:
- Do we want to [_partition_][4] the tables by date?
- Is there any reason to create separate tables for the separate archive sources? (‘list’ source is preserved as a field in the records).  Currently, this doesn’t seem necessary.

### Running the script
(Instructions TBD).

#### Processing new archive files

As we add files to the archive buckets, we can use GCS notifications to trigger running a Cloud Function that kicks off the script with the new files.

## ‘entity extraction’ scripts

The [`dataflow_names_emails.py`][5] script is a Beam/Dataflow pipeline that finds all the names associated with an email, and all the emails associated with a name, and outputs the results to two tables. 

The [`extract_entities.py`][6] script is an experiment in progress, to play with ways to identify the names and emails for a given person and aggregate them to create ‘entities’.  As an example of where this would be useful, see this query:

```sql
SELECT from_name, from_email
FROM `project-ocean-281819.mail_archives.names_emails`
WHERE REGEXP_CONTAINS(from_name, 'warsaw')
```

This is the query result.  
<figure>
<a href="https://storage.googleapis.com/amy-jo/images/Screen%20Shot%202020-09-18%20at%2011.18.26%20AM.png" target="_blank"><img src="https://storage.googleapis.com/amy-jo/images/Screen%20Shot%202020-09-18%20at%2011.18.26%20AM.png" width="40%"/></a>
<figcaption><br/><i><small>The same person may use many different email addresses and names.</small></i></figcaption>
</figure>

There are many such. We’d like to be able to identify messages with all of these different header configs as associated with the same person.

The entity extraction script dumps its results into a BQ table.

## TODOs
(in addition to general script cleanup).

- add the ability to pass list of files to the ingestion script to process (currently, is either single file or whole bucket).
- Create and check in GCF definition/setup instructions, for processing new archive files.


[1]:	./extract_msgs.py
[2]:	./table_schema.json
[3]:	./table_schema.json
[4]:	https://cloud.google.com/bigquery/docs/partitioned-tables
[5]:	./dataflow/dataflow_names_emails.py
[6]:	./extract_entities.py