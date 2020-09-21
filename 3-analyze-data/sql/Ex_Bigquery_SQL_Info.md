<!-- Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. -->

## project-OCEAN SQL Information for Data Analysis 

Example SQL to explore metadata for existing BigQuery repos using Libraries.io dataset.

Also checkout this [post](https://medium.com/google-cloud/bigquery-dataset-metadata-queries-8866fa947378) that walks through these BigQuery queries.

### Naming Best Practices

When saving sql queries, views, datasets or tables use the following naming convention:
*nameoffile-4digyear-month-day-initials*

* Use a descriptive name to make it clear what you are saving
* Put the date in order of year-month-date
* Include your initials and add middle initial if you have it
* Use - where you can and _ if you can’t


### Exampl Metadata Queries

Query to get table schema.

    SELECT * FROM bigquery-public-data.libraries_io.INFORMATION_SCHEMA.TABLES


Query for more detailed table schema info including lastest update date, number of rows and size.

    SELECT s.table_catalog, s.table_schema, s.table_name, s.table_type, s.is_insertable_into, s.is_typed, CAST(TIMESTAMP_MILLIS(s.creation_time) AS DATETIME) as creation_time, CAST(TIMESTAMP_MILLIS(t.last_modified_time) AS DATETIME) as last_modified_time, t.row_count, t.size_bytes / POW(10,9) as GB, t.type FROM bigquery-public-data.libraries_io.INFORMATION_SCHEMA.TABLES s 
    JOIN bigquery-public-data.libraries_io.__TABLES__ t 
    ON s.table_name = t.table_id


Query to get column information for a table.

    SELECT * 
    FROM bigquery-public-data.libraries_io.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = "repositories"


