# -*- coding: utf-8 -*-
#
#   Copyright 2018 Enigmo Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
  
  JDBC to Google Cloud Storage plugin

  This plugin provides JdbcToGoogleCloudStorageOperator.
  It is modified from MySqlToGoogleCloudStorageOperator provided at

    https://github.com/apache/incubator-airflow/blob/e028b7c1467ffd160cf6c4e443be446aae44f081/airflow/contrib/operators/mysql_to_gcs.py

"""

import sys
import json
import time
import base64

from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.jdbc_hook import JdbcHook
from datetime import date, datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile
from six import string_types
import logging
import time
import jaydebeapi
import gzip
import os
import pendulum

PY3 = sys.version_info[0] == 3
UTC_TZ = pendulum.timezone('UTC')

class JdbcToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from DB to Google cloud storage using JDBC in JSON format.
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 gzip_compression=False,
                 schema_filename=None,
                 fetch_size=1000,
                 approx_max_file_size_bytes=1900000000,
                 datetime_timezone=None,
                 jdbc_conn_id='jdbc_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the JDBC table.
        :type sql: string
        :param bucket: The bucket to upload to.
        :type bucket: string
        :param filename: The filename to use as the object name when uploading
            to Google cloud storage. A {} should be specified in the filename
            to allow the operator to inject file numbers in cases where the
            file is split due to size.
        :type filename: string
        :param jdbc_conn_id: Reference to a specific JDBC hook.
        :type jdbc_conn_id: string
        :param gzip_compression: Whether to compress data files. If set,
            `approx_max_file_size_bytes` is treated as pre-compression size.
        :type gzip_compression: bool
        :param schema_filename: If set, the filename to use as the object name
            when uploading a .json file containing the BigQuery schema fields
            for the table that was dumped from JDBC.
        :type schema_filename: string
        :param fetch_size: The number of rows to fetch at a time.
        :type fetch_size: long
        :param approx_max_file_size_bytes: This operator supports the ability
            to split large table dumps into multiple files (see notes in the
            filenamed param docs above). Google cloud storage allows for files
            to be a maximum of 4GB. This param allows developers to specify the
            file size of the splits.
        :type approx_max_file_size_bytes: long
        :param datetime_timezone: If set, all datetime values are convert to UTC
            assuming the datetime values from DB are local time in this timezone.
        :type datetime_timezone: string
        :param jdbc_conn_id: Reference to a specific JDBC hook.
        :type jdbc_conn_id: string
        :param google_cloud_storage_conn_id: Reference to a specific Google
            cloud storage hook.
        :type google_cloud_storage_conn_id: string
        :param delegate_to: The account to impersonate, if any. For this to
            work, the service account making the request must have domain-wide
            delegation enabled.
        """
        super(JdbcToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.gzip_compression = gzip_compression
        self.schema_filename = schema_filename
        self.fetch_size = fetch_size
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        if datetime_timezone:
            self.datetime_timezone=pendulum.timezone(datetime_timezone)
        else:
            self.datetime_timezone=None
        self.jdbc_conn_id = jdbc_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        cursor = self._query_jdbc()
        data_files = self._write_local_data_files(cursor)
        if not data_files:
            self.log.info('Skip the task because no data found.')
            raise AirflowSkipException('No data found.')
        data_file_mime_type = 'application/json'
        if self.gzip_compression:
            data_file_mime_type = 'application/gzip'
        self._upload_to_gcs(data_files, data_file_mime_type)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            schema_file = self._write_local_schema_file(cursor)
            self._upload_to_gcs(schema_file, 'application/json')

    def _query_jdbc(self):
        """
        Queries jdbc and returns a cursor to the results.
        """
        jdbc = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        conn = jdbc.get_conn()
        cursor = conn.cursor()
        self.log.info('Querying SQL: %s', self.sql)
        cursor.execute(self.sql)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.
        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        col_types = list(map(lambda schema_tuple: schema_tuple[1], cursor.description))
        col_names = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        file_no = 0
        tmp_file_handle = None
        tmp_file_handles = {}
        self.log.info('Fetching rows. Fetch Size: %s', self.fetch_size)
        while True:
            rows = cursor.fetchmany(self.fetch_size)
            if not rows:
                if tmp_file_handle:
                    tmp_file_handle.close()
                break
            if not tmp_file_handle:
                object_name = self.filename.format(file_no)
                tmp_file_handle = self._data_open_temporary_file()
                tmp_file_handles[object_name] = tmp_file_handle
                self.log.info('Writing local file: %s, object name: %s', tmp_file_handle.name, object_name)
            for row in rows:
                # Convert boolean to integer, decimals to floats and timezone of datetime to utc.
                row = self._convert_values(col_types, row)
                row_dict = dict(zip(col_names, row))


                # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
                s = json.dumps(row_dict)
                if PY3:
                    s = s.encode('utf-8')
                tmp_file_handle.write(s)

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b'\n')

                # Stop if the file exceeds the file size limit.
                if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                    tmp_file_handle.close()
                    file_no += 1
                    object_name = self.filename.format(file_no)
                    tmp_file_handle = self._data_open_temporary_file()
                    tmp_file_handles[object_name] = tmp_file_handle
                    self.log.info('Writing local file: %s, object name: %s', tmp_file_handle.name, object_name)
        return tmp_file_handles

    def _data_open_temporary_file(self):
        suffix=None
        if self.gzip_compression:
            suffix='.gz'
        tmp_file_handle = NamedTemporaryFile(delete=False, suffix=suffix)
        if self.gzip_compression:
            tmp_file_handle = gzip.GzipFile(fileobj=tmp_file_handle)
        return tmp_file_handle


    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.
        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema_str = None
        tmp_schema_file_handle = NamedTemporaryFile(delete=False)
        schema = []
        for field in cursor.description:
            # See PEP 249 for details about the description tuple.
            field_name = field[0]
            field_type = self.type_map(field[1])
            # Always allow TIMESTAMP to be nullable. jaydebeapi returns None types
            # for required fields because some DB timestamps can't be
            # represented by Python's datetime (e.g. 0000-00-00 00:00:00).
            if field[6] or field_type == 'TIMESTAMP':
                field_mode = 'NULLABLE'
            else:
                field_mode = 'REQUIRED'
            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': field_mode,
            })
        schema_str = json.dumps(schema)
        if PY3:
            schema_str = schema_str.encode('utf-8')
        tmp_schema_file_handle.write(schema_str)
        tmp_schema_file_handle.close()
        self.log.info('Writing local temp file: %s', tmp_schema_file_handle.name)
        self.log.info('Using schema for %s: %s', self.schema_filename, schema_str)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload, mime_type):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object, tmp_file_handle.name, mime_type)
            os.remove(tmp_file_handle.name)


    def _convert_values(self, col_types, row):
        """
        Takes a value from JDBC, and converts it to a value that's safe for
        JSON/Google cloud storage/BigQuery. Booleans are converted to integers.
        Timezone of datetimes are converted to UTC. Decimals are converted to floats.
        """
        converted_row = []
        for col_type, col_val in zip(col_types, row):
            if isinstance(col_val, bool):
                col_val = int(col_val)
            elif isinstance(col_val, Decimal):
                col_val = float(col_val)
            elif isinstance(col_val, str) and self.datetime_timezone and col_type == jaydebeapi.DATETIME:
                try:
                    naive_datetime = datetime.strptime(col_val, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    # sometimes microsecond part is missing.
                    naive_datetime = datetime.strptime(col_val, "%Y-%m-%d %H:%M:%S")
                aware_datetime = self.datetime_timezone.convert(naive_datetime)
                utc_datetime = UTC_TZ.convert(aware_datetime)
                try:
                    col_val = datetime.strftime(utc_datetime, "%Y-%m-%d %H:%M:%S.%fZ")
                except ValueError:
                    col_val = None
            else:
                col_val = col_val
            converted_row.append(col_val)
        return converted_row


    @classmethod
    def type_map(cls, jdbc_type):
        """
        Helper function that maps from JDBC fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            jaydebeapi.STRING: 'STRING',
            jaydebeapi.TEXT: 'STRING',
            jaydebeapi.BINARY: 'BYTES',
            jaydebeapi.NUMBER: 'INTEGER',
            jaydebeapi.FLOAT: 'FLOAT',
            jaydebeapi.DECIMAL: 'FLOAT',
            jaydebeapi.DATE: 'TIMESTAMP',
            jaydebeapi.TIME: 'TIMESTAMP',
            jaydebeapi.DATETIME: 'TIMESTAMP',
            jaydebeapi.ROWID: 'INTEGER'
        }
        return d[jdbc_type] if jdbc_type in d else 'STRING'

class JdbcToGoogleCloudStoragePlugin(AirflowPlugin):
    name = "jdbc_to_gcs"
    operators = [JdbcToGoogleCloudStorageOperator]

