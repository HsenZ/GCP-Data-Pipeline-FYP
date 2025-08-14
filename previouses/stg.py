import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import datetime
import hashlib
import csv
from io import StringIO

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

        # pad missing values with None
        values += [None] * (len(COLUMNS) - len(values))

        row = {}
        for col, val in zip(COLUMNS, values):
            val = val.strip() if val else None
            if col in ['latitude', 'longitude', 'depth', 'mag', 'gap', 'dmin', 'rms', 'horizontalError', 'depthError', 'magError']:
                val = float(val) if val not in [None, ''] else 0.0
            elif col in ['nst', 'magNst']:
                val = int(val) if val not in [None, ''] else 0
            row[col] = val

        # Include the original line only for rejection writing
        row['original_csv_line'] = line
        yield row

class FilterTransform(beam.DoFn):
    def process(self, row):
        try:
            depth = row.get('depth', 0.0)
            magError = row.get('magError', 0.0)
            depthError = row.get('depthError', 0.0)
            mag = row.get('mag', 0.0)
            eq_type = row.get('type', '')

            if eq_type == 'earthquake' and (depth < 1 or magError > 0.5 or depthError > 30 or mag < 1):
                # Yield to rejected tagged output with original CSV line
                yield beam.pvalue.TaggedOutput('rejected', row['original_csv_line'])
                return

            # Add technical columns
            time_str = row.get('time', '')
            lat_str = str(row.get('latitude', ''))
            long_str = str(row.get('longitude', ''))
            hash_input = time_str + lat_str + long_str + current_date
            execution_id = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

            row['insertion_date'] = int(current_date)
            row['updated_date'] = None
            row['execution_id'] = execution_id
            row['data_source'] = f'all_earthquakes_{current_date}'

            # Remove the original_csv_line before writing to BigQuery to avoid schema error
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

    table_schema = {
        "fields": [
            {"name": "time", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "updated", "type": "STRING", "mode": "NULLABLE"},
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "horizontalError", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depthError", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magError", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magNst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "locationSource", "type": "STRING", "mode": "NULLABLE"},
            {"name": "magSource", "type": "STRING", "mode": "NULLABLE"},
            {"name": "insertion_date", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "updated_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "execution_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "data_source", "type": "STRING", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | 'Read CSV lines' >> beam.io.ReadFromText(FILE_PATH, skip_header_lines=1)
            | 'Parse CSV with csv.reader' >> beam.ParDo(ParseCSVLine())
            | 'Filter and Add Columns' >> beam.ParDo(FilterTransform()).with_outputs('rejected', main='accepted')
        )

        accepted = parsed.accepted
        rejected = parsed.rejected

        # Write accepted records to BigQuery
        accepted | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.{TABLE}',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Write rejected rows to GCS as CSV
        rejected | 'Write rejected to GCS' >> beam.io.WriteToText(
            f'gs://{BUCKET}/REJECTED/{current_date}/rejected_from_{current_date}_all_day',
            file_name_suffix='.csv',
            shard_name_template=''  # no sharding, single file
        )

if __name__ == '__main__':
    run()
