import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.filesystems import FileSystems
import csv
from io import StringIO
import os
from datetime import datetime, timedelta, timezone
from apache_beam.io.gcp import gce_metadata_util

# ───────────── CONFIG ─────────────
PROJECT_ID = 'imp-fyp'
BUCKET = 'imp-bucket'
RAW_PATTERN = 'gs://imp-bucket/RAWDATA/whole_month_*.csv'
DATASET = 'STG_ds'
TABLE = 'T_STG_day_earthquake'
REGION = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'
# ──────────────────────────────────

class _AttachJobId(beam.DoFn):
    """Inject the current Dataflow job-ID into each element."""
    def setup(self):
        self.job_id = (
            getattr(gce_metadata_util, "fetch_dataflow_job_id", None)()
            or getattr(gce_metadata_util, "_fetch_custom_gce_metadata",
                       lambda x: "")("job_id")
            or os.getenv("DATAFLOW_JOB_ID")
            or os.getenv("JOB_ID")
            or "LOCAL_RUN"
        )
    def process(self, element):
        element["job_execution_id"] = self.job_id     
        yield element
        
        
# Get latest matching raw CSV file path
def latest_raw_csv(pattern: str) -> str:
    match = FileSystems.match([pattern])[0].metadata_list
    if not match:
        raise FileNotFoundError(f"No file matched: {pattern}")
    def updated_ts(meta):
        return getattr(meta, 'last_updated', getattr(meta, 'mtime', 0))
    newest = max(match, key=updated_ts)
    return newest.path

FILE_PATH = latest_raw_csv(RAW_PATTERN)
raw_filename = FILE_PATH.split('/')[-1]
print(f"Using latest raw file: {FILE_PATH}")

EEST = timezone(timedelta(hours=3))
timestamp = datetime.now(EEST).strftime('%Y%m%d-%H%M%S')

COLUMNS = [
    "time", "latitude", "longitude", "depth", "mag", "magType", "nst", "gap",
    "dmin", "rms", "net", "id", "updated", "place", "type",
    "horizontalError", "depthError", "magError", "magNst", "status",
    "locationSource", "magSource"
]

class ParseCSVLine(beam.DoFn):
    def process(self, line):
        reader = csv.reader(StringIO(line))
        values = next(reader)
        values += [''] * (len(COLUMNS) - len(values))
        row = {col: (val.strip() if val else '') for col, val in zip(COLUMNS, values)}
        row['original_csv_line'] = line
        yield row

class FilterTransform(beam.DoFn):
    def __init__(self, raw_filename):
        self.raw_filename = raw_filename

    def setup(self):
        self.job_id = os.environ.get("DATAFLOW_JOB_ID") or os.environ.get("JOB_ID") or "LOCAL_RUN"

    def process(self, row):
        try:
            for k, v in row.items():
                v = v.strip() if isinstance(v, str) else v
                if v in ['', '0', 0]:
                    row[k] = None
                else:
                    row[k] = v

            depth = float(row.get('depth') or 0)
            mag_error = float(row.get('magError') or 0)
            depth_error = float(row.get('depthError') or 0)
            mag = float(row.get('mag') or 0)
            eq_type = row.get('type') or ''

            if eq_type == 'earthquake' and (depth < 1 or mag_error > 0.5 or depth_error > 30 or mag < 1):
                yield beam.pvalue.TaggedOutput('rejected', row['original_csv_line'])
                return

            row.update({
                'insertion_date': timestamp,
                'updated_date': None,
                'job_execution_id': self.job_id,
                'data_source': self.raw_filename
            })

            row.pop('original_csv_line', None)
            row = {k: (str(v) if v is not None else None) for k, v in row.items()}
            yield row

        except Exception as e:
            print("Error processing row:", e)

def run():
    options = PipelineOptions()
    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    gco.job_name = f'staging-full-load-{timestamp.lower()}'
    gco.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    gco.temp_location = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    print(f"Pipeline execution STARTED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | 'Read CSV' >> beam.io.ReadFromText(FILE_PATH, skip_header_lines=1)
            | 'Parse rows' >> beam.ParDo(ParseCSVLine())
            | 'Filter & Enrich' >> beam.ParDo(FilterTransform(raw_filename)).with_outputs('rejected', main='accepted')
        )
        
        (
        parsed.accepted 
        | 'Attach Job-ID to accepted' >> beam.ParDo(_AttachJobId())
        | 'Write to BQ' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.{TABLE}',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
        )

        (
        parsed.rejected 
        | 'Write Rejected Rows' >> beam.io.WriteToText(
            f'gs://{BUCKET}/REJECTED/{timestamp}/rejected_from_{timestamp}_all_day',
            file_name_suffix='.csv',
            shard_name_template=''
        )
        )


    print(f"Pipeline execution FINISHED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")

if __name__ == '__main__':
    run()
