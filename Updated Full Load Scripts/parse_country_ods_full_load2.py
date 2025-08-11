import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
from apache_beam.pvalue import AsDict
from datetime import datetime, timedelta, timezone
import re
import os
import pytz
from apache_beam.io.gcp import gce_metadata_util

# ───────────── CONFIG ─────────────
PROJECT_ID = 'imp-fyp'
DATASET = 'ODS_ds'
INPUT_TABLE = 'ods_day_earthquake'
OUTPUT_TABLE = 'T_ODS_day_earthquake'
STATES_TABLE = '50-US-States'
REGION = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'
# ──────────────────────────────────

class _AttachJobId(beam.DoFn):
    """Stamp the current Dataflow job-ID on every element."""
    def setup(self):
        self.job_id = (
            getattr(gce_metadata_util, "fetch_dataflow_job_id", None)()
            or getattr(gce_metadata_util, "_fetch_custom_gce_metadata", lambda x: "")("job_id")
            or os.getenv("DATAFLOW_JOB_ID")
            or os.getenv("JOB_ID")
            or "LOCAL_RUN"
        )
    def process(self, element):
        element["_LB_job_execution_id"] = self.job_id   # overwrite/insert
        yield element


class ParsePlaceDoFn(beam.DoFn):
    def process(self, row, states_dict):
        place = row.get("LB_place", "")
        updated_row = dict(row)

        # ✅ Set insertion & updated timestamps (EEST, as datetime objects)
        eest = pytz.timezone('Europe/Bucharest')
        now_eest = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

        updated_row["_DT_insertion_date"] = now_eest

        # Default to null for both
        updated_row["LB_Region"] = None
        updated_row["LB_Country"] = None

        if not place:
            yield updated_row
            return

        if "," not in place:
            # No comma: treat whole string as region (non-US)
            updated_row["LB_Region"] = place.strip()
        else:
            parts = place.rsplit(",", 1)
            left_part = parts[0].strip()
            rl = parts[1].strip()  # Candidate for US state or non-US country

            state_match = states_dict.get(rl)
            if state_match:
                updated_row["LB_Region"] = state_match
                updated_row["LB_Country"] = "USA"
            else:
                match = re.search(r"of\s+(.+)$", left_part, flags=re.IGNORECASE)
                if match:
                    rr = match.group(1).strip()
                else:
                    rr = left_part.strip()
                updated_row["LB_Region"] = rr
                updated_row["LB_Country"] = rl

        yield updated_row

def run():
    EEST = timezone(timedelta(hours=3))
    print(f"Pipeline execution STARTED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")
    options = PipelineOptions()
    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    gco.job_name = 'parse-country-ods-full-load'
    gco.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    gco.temp_location = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        # Load US states: key = state or abbreviation, value = full State name
        us_states = (
            p
            | 'Read US States Table' >> ReadFromBigQuery(
                table=f'{PROJECT_ID}:{DATASET}.{STATES_TABLE}',
                use_standard_sql=True)
            | 'Map States to Dict' >> beam.FlatMap(lambda row: [
                (row['State'].strip(), row['State']),
                (row['Abbreviation'].strip(), row['State'])
            ])
        )

        states_dict = AsDict(us_states)

        # Read ODS input table
        rows = (
            p
            | 'Read ODS Table' >> ReadFromBigQuery(
                table=f'{PROJECT_ID}:{DATASET}.{INPUT_TABLE}',
                use_standard_sql=True)
        )

        # Transform place into region/country + update timestamp
        enriched = (
            rows
            | 'Parse Country Info' >> beam.ParDo(ParsePlaceDoFn(), states_dict)
            | 'Attach Job-ID'      >> beam.ParDo(_AttachJobId())
        )

        # Write final enriched rows (truncate = full load)
        enriched | 'Write to Output Table' >> WriteToBigQuery(
            table=f'{PROJECT_ID}:{DATASET}.{OUTPUT_TABLE}',
            schema=None,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE
        )
    
    print(f"Pipeline execution FINISHED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")

if __name__ == '__main__':
    run()
