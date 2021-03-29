import csv
import os
import argparse
import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions


def print_row(element):
    print(element)


def parse_file(element):
    for line in csv.reader([element], quotechar='"', delimiter=',',
                           quoting=csv.QUOTE_ALL, skipinitialspace=True):
        return line


dir_path = os.path.dirname(os.path.realpath(__file__))
input_filename = os.path.join(dir_path, 'json_schema.json')
parser = argparse.ArgumentParser()
parser.add_argument(
                    '--input',
                    dest='input',
                    required=False,
                    help='Input file to read.  This can be a local file or '
                    'a file in a Google Storage Bucket.',
                    default='gs://video_watched/pipelines/sample.csv')
parser.add_argument(
                    '--output',
                    dest='output',
                    required=False,
                    help='Output BQ table to write results to.',
                    default='infotrack-videowatch:video_watched.video_watched')

known_args, pipeline_args = parser.parse_known_args(None)

p = beam.Pipeline(options=PipelineOptions(pipeline_args,
                  region='australia-southeast1'))

parsed_csv = (p
              | 'Read input file' >> beam.io.ReadFromText(input_filename)
                | 'Parse file' >> beam.Map(parse_file)
                | 'Print output' >> beam.Map(print_row)
              )

result = p.run()
result.wait_until_finish()