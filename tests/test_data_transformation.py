from src.data_transformation import DataTransformation
import csv
import os
from datetime import datetime
import argparse
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class TestDataTransformation(object):
    dt = DataTransformation()

    def test_data_transformation_init_with_not_null_schema(self):
        actual = self.dt.schema_str
        message = "Init with not null schema from JSON"
        assert actual is not None, message

    def test_data_transformation_transform_row_with_event_206(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,206']
        actual = self.dt.transform_row(csv_row, field_map)
        message = "The row with events including 206 should not be transformed"
        assert actual == {}, message

    def test_data_transformation_transform_row_with_split_VideoTitle(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,206']
        actual = self.dt.transform_row(csv_row, field_map)
        message = "The row with only one element when split the VideoTitle" +\
                  "should not be transformed"
        assert actual == {}, message

    def test_data_transformation_transform_row_metric(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = [{'event': '127'}, {'event': '157'}, {'event': '120'}]
        message = "Row['metric'] is on the right format"
        assert actual['metric'] == expected, message

    def test_data_transformation_transform_row_date_watch(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = datetime.strptime('2017-01-11T00:00:22.000Z',
                                     '%Y-%m-%dT%H:%M:%S.%fZ').date()
        message = "Row['date_watch'] is on the right format"
        assert actual['date_watch'] == expected, message

    def test_data_transformation_transform_row_time_watch(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = datetime.strptime('2017-01-11T00:00:22.000Z',
                                     '%Y-%m-%dT%H:%M:%S.%fZ').time()
        message = "Row['time_watch'] is on the right format"
        assert actual['time_watch'] == expected, message

    def test_data_transformation_transform_row_platform_Desktop(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = 'Desktop'
        message = "Row['platform'] is on the right format <<Desktop>>"
        assert actual['platform'] == expected, message

    def test_data_transformation_transform_row_platform_Iphone(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'App Iphone| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = 'Iphone'
        message = "Row['platform'] is on the right format <<Iphone>>"
        assert actual['platform'] == expected, message

    def test_data_transformation_transform_row_site(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'http://abc.com| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = 'http://abc.com'
        message = "Row['site'] is on the right format <<Iphone>>"
        assert actual['site'] == expected, message

    def test_data_transformation_transform_row_site_null(self):
        schema = parse_table_schema_from_json(self.dt.schema_str)
        field_map = [f for f in schema.fields]

        csv_row = ['2017-01-11T00:00:22.000Z',
                   'news| Australian pedophile extradited to US',
                   '127,157,120,160,104,162,161,163,164,165,166,171,229']
        actual = self.dt.transform_row(csv_row, field_map)
        expected = 'null'
        message = "Row['site'] is on the right format <<Iphone>>"
        assert actual['site'] == expected, message

    def test_data_transformation_pipeline(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        test_file = os.path.join(dir_path, 'test.csv')
        parser = argparse.ArgumentParser()
        parser.add_argument(
                        '--input',
                        dest='input',
                        required=False,
                        help='Input file to read. This can be a local file or '
                        'a file in a Google Storage Bucket.',
                        default=test_file)
        parser.add_argument(
                        '--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='None')

        known_args, pipeline_args = parser.parse_known_args()
        schema = parse_table_schema_from_json(self.dt.schema_str)
        video_date_time = '2017-01-11T00:00:31.000Z'
        video_date_watch = datetime.strptime(video_date_time,
                                             '%Y-%m-%dT%H:%M:%S.%fZ').date()
        video_time_watch = datetime.strptime(video_date_time,
                                             '%Y-%m-%dT%H:%M:%S.%fZ').time()
        expected = [{'metric': [{'event': '157'}, {'event': '120'}],
                     'date_watch': video_date_watch,
                     'time_watch': video_time_watch,
                     'platform': 'Web',
                     'site': 'null',
                     'video': 'William Tyrrell twist'}]
        with TestPipeline(options=PipelineOptions(pipeline_args,
                          region='australia-southeast1',
                          save_main_session=True)) as p:
            input = p | beam.io.ReadFromText(known_args.input,
                                             skip_header_lines=1)
            output = input | beam.ParDo(lambda s: self.dt.parse_method(s))

            assert_that(output, equal_to(expected), label='CheckOutput')
