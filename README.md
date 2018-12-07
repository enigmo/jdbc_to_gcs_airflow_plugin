# JDBC to GCS Airflow Plugin

This plugin provides an operator that moves data from DBs to Google Cloud Storage using JdbcHook.

## Operators

### JdbcToGoogleCloudStorageOperator

This operator reads data from DBs using JdbcHook and transfers it to Google Cloud Storage.

####  Parameters:

 * **sql**(string) - The SQL to execute on the JDBC table.
 * **bucket**(string) -  The bucket to upload to.
 * **filename**(string) - The filename to use as the object name when uploading to Google Cloud Storage. A {} should be specified in the filename to allow the operator to inject file numbers in cases where the file is split due to size.
 * **jdbc_conn_id**(string) - Reference to a specific JDBC hook.
 * **gzip_compression**(bool) - Whether to compress data files. If set, `approx_max_file_size_bytes` is treated as pre-compression size.
 * **schema_filename**(string) - If set, the filename to use as the object name when uploading a .json file containing the BigQuery schema fields for the table that was dumped from JDBC.
 * **fetch_size**(long) - The number of rows to fetch at a time.
 * **approx_max_file_size_bytes**(long) - This operator supports the ability to split large table dumps into multiple files (see notes in the filenamed param docs above). Google Cloud Storage allows for files to be a maximum of 4GB. This param allows developers to specify the file size of the splits.
 * **datetime_timezone**(string) If set, all datetime values are convert to UTC assuming the datetime values from DB are local time in this timezone.
 * **jdbc_conn_id**(string) - Reference to a specific JDBC hook.
 * **google_cloud_storage_conn_id**(string) -  Reference to a specific Google Cloud Storage hook.
 * **delegate_to**(string) -  The account to impersonate, if any. For this to work, the service account making the request must have domain-wide delegation enabled.

## License

JDBC to GCS Airflow Plugin is licensed under the Apache License, Version2.0