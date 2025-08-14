import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import hashlib
import csv
from io import StringIO
import datetime

# --------------- CONFIGURE THESE PARAMETERS -------------------
PROJECT_ID = 'imp-fyp'
BUCKET = 'imp-bucket'
FILE_PATH = 'gs://imp-bucket/all_day.csv'
DATASET = 'stg_ds'
TABLE = 'stg_day_earthquake'
REGION = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'
# --------------------------------------------------------------

COLUMNS = ["time", "latitude", "longitude", "depth", "mag", "magType", "nst", "gap", "dmin", "rms",
           "net", "id", "updated", "place", "type", "horizontalError", "depthError", "magError",
           "magNst", "status", "locationSource", "magSource"]

current_date = datetime.datetime.utcnow().strftime('%Y%m%d')

class ParseCSVLine(beam.DoFn):
    def process(self, line):
        f = StringIO(line)
        reader = csv.reader(f)
        values = next(reader)
        values += [None] * (len(COLUMNS) - len(values))

        row = {col: None for col in COLUMNS}
        for col, val in zip(COLUMNS, values):
            if col == 'place':
                row[col] = val
                continue
            val = val.strip() if val else None
            if col in ['latitude', 'longitude', 'depth', 'mag', 'gap', 'dmin', 'rms', 'horizontalError', 'depthError', 'magError']:
                row[col] = float(val) if val not in [None, ''] else None
            elif col in ['nst', 'magNst']:
                row[col] = int(val) if val not in [None, ''] else None
            else:
                row[col] = val if val not in [None, ''] else None

        row['original_csv_line'] = line
        yield row

class FilterTransform(beam.DoFn):
    def process(self, row):
        try:
            depth = row.get('depth', 0.0) or 0.0
            magError = row.get('magError', 0.0) or 0.0
            depthError = row.get('depthError', 0.0) or 0.0
            mag = row.get('mag', 0.0) or 0.0
            eq_type = row.get('type', '') or ''

            if eq_type == 'earthquake' and (depth < 1 or magError > 0.5 or depthError > 30 or mag < 1):
                yield beam.pvalue.TaggedOutput('rejected', row['original_csv_line'])
                return

            time_str = row.get('time', '')
            lat_str = str(row.get('latitude', ''))
            long_str = str(row.get('longitude', ''))
            # hash_input = time_str + lat_str + long_str + current_date
            # execution_id = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

            row['insertion_date'] = int(current_date)
            row['updated_date'] = None
            row['execution_id'] = execution_id
            row['data_source'] = f'all_earthquakes_{current_date}'
            row.pop('original_csv_line', None)

            yield row

        except Exception as e:
            print("Error processing row:", e)

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.region = REGION
    google_cloud_options.job_name = f'earthquake-staging-pipeline-{current_date}'
    google_cloud_options.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    google_cloud_options.temp_location = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | 'Read CSV lines' >> beam.io.ReadFromText(FILE_PATH, skip_header_lines=1)
            | 'Parse CSV with csv.reader' >> beam.ParDo(ParseCSVLine())
            | 'Filter and Add Columns' >> beam.ParDo(FilterTransform()).with_outputs('rejected', main='accepted')
        )

        accepted = parsed.accepted
        rejected = parsed.rejected

        accepted | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.{TABLE}',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

        rejected | 'Write rejected to GCS' >> beam.io.WriteToText(
            f'gs://{BUCKET}/REJECTED/{current_date}/rejected_from_{current_date}_all_day',
            file_name_suffix='.csv',
            shard_name_template=''
        )

if __name__ == '__main__':
    run()