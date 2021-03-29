import os
import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools \
    import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions

from datetime import datetime
import re
import csv


class DataTransformation:
    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.schema_str = ''
        schema_file = os.path.join(dir_path, 'json_schema.json')
        with open(schema_file) as f:
            data = f.read()
            self.schema_str = '{"fields": ' + data + '}'

    def _is_valid_url(self, str):
        regex = ("((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]" +
                 "{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)")
        p = re.compile(regex)
        if (str is None):
            return False
        if(re.search(p, str)):
            return True
        else:
            return False

    def transform_row(self, csv_row, field_map):
        row = {}
        _IGNORE_EVENT = '206'
        video_date_time_obj = datetime.strptime(csv_row[0].strip(),
                                                '%Y-%m-%dT%H:%M:%S.%fZ')
        list_video_title = csv_row[1].strip().split('|')
        video_watched_metric = \
            [{'event': e} for e in csv_row[2].strip().split(',')]
        if (_IGNORE_EVENT not in csv_row[2]) \
                and (len(list_video_title) > 1):
            row[field_map[0].name] = video_date_time_obj.date()
            row[field_map[1].name] = video_date_time_obj.time()
            if 'App' in list_video_title[0]:
                row[field_map[2].name] = \
                    list_video_title[0].split(' ')[1].strip()
            else:
                row[field_map[2].name] = 'Desktop'
            if self._is_valid_url(list_video_title[0]):
                row[field_map[3].name] = list_video_title[0].strip()
            else:
                row[field_map[3].name] = 'null'
            row[field_map[4].name] = list_video_title[-1].strip()
            row[field_map[5].name] = video_watched_metric
        return row

    def parse_method(self, string_input):
        schema = parse_table_schema_from_json(self.schema_str)
        field_map = [f for f in schema.fields]

        reader = csv.reader(string_input.split('\n'), skipinitialspace=True,
                            delimiter=',', quotechar='"')
        for csv_row in reader:
            row = self.transform_row(csv_row, field_map)
            if row:
                yield row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
                    '--input',
                    dest='input',
                    required=False,
                    help='Input file to read. This can be a local file or '
                    'a file in a Google Storage Bucket.',
                    default='gs://video_watch/data/video_data.csv')
    parser.add_argument(
                    '--output',
                    dest='output',
                    required=False,
                    help='Output BQ table to write results to.',
                    default='infotrack-videowatchanalytic:' +
                            'video_watch.video_watch')

    known_args, pipeline_args = parser.parse_known_args(argv)
    data_ingestion = DataTransformation()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args,
                      region='australia-southeast1', save_main_session=True))
    schema = parse_table_schema_from_json(data_ingestion.schema_str)

    (p
     | 'Read From Text' >>
     beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Transform Data and Convert to BigQuery Row' >>
     beam.ParDo(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
         known_args.output, schema=schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
