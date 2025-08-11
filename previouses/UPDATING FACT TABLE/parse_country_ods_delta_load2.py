import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions
)
from apache_beam.io.gcp.bigquery import (
    ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
)
from apache_beam.pvalue import AsDict
from apache_beam.metrics import Metrics
from datetime import datetime, timezone, timedelta
import re, os, logging, pytz

from apache_beam.io.gcp import gce_metadata_util

# ───────────── CONFIG ─────────────
PROJECT_ID     = 'imp-fyp'
DATASET        = 'ODS_ds'
INPUT_TABLE    = 'ods_day_earthquake'       # source (already enriched with mag/depth etc.)
OUTPUT_TABLE   = 'T_ODS_day_earthquake'     # destination (delta-load target)
STATES_TABLE   = '50-US-States'
REGION         = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'
# ──────────────────────────────────

# ───────────── JOB-ID helper ───────
class _AttachJobId(beam.DoFn):
    def setup(self):
        self.job_id = (
            getattr(gce_metadata_util, "fetch_dataflow_job_id", None)()
            or getattr(gce_metadata_util, "_fetch_custom_gce_metadata", lambda x: "")("job_id")
            or os.getenv("DATAFLOW_JOB_ID")
            or os.getenv("JOB_ID")
            or "LOCAL_RUN"
        )
    def process(self, element):
        element["_LB_job_execution_id"] = self.job_id
        yield element

# ───────────── Place → Region/Country ─────────────
class ParsePlaceDoFn(beam.DoFn):
    def __init__(self):
        self.rows_in   = Metrics.counter('parse', 'rows_in')
        self.rows_out  = Metrics.counter('parse', 'rows_out')

    def process(self, row, states_dict):
        self.rows_in.inc()
        updated_row = dict(row)

        place = updated_row.get("LB_place", "")

        # set insertion & updated timestamp (EEST, always now)
        eest = pytz.timezone('Europe/Bucharest')
        now_eest = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

        updated_row["_DT_insertion_date"] = now_eest
        updated_row["_DT_updated_date"]   = None 

        # default nulls
        updated_row["LB_Region"]  = None
        updated_row["LB_Country"] = None

        if place:
            if "," not in place:
                updated_row["LB_Region"] = place.strip()
            else:
                left, right = place.rsplit(",", 1)
                rl = right.strip()                   # candidate US state or non-US country
                state_match = states_dict.get(rl)
                if state_match:
                    updated_row["LB_Region"]  = state_match
                    updated_row["LB_Country"] = "USA"
                else:
                    match = re.search(r"of\s+(.+)$", left, flags=re.IGNORECASE)
                    rr = match.group(1).strip() if match else left.strip()
                    updated_row["LB_Region"]  = rr
                    updated_row["LB_Country"] = rl

        self.rows_out.inc()
        yield updated_row

# ───────────── Deduplicate step ─────────────
class DropExistingIds(beam.DoFn):
    def __init__(self):
        self.dup   = Metrics.counter('parse', 'rows_filtered')
        self.write = Metrics.counter('parse', 'rows_written')

    def process(self, element, existing_ids):
        if element['ID_Event'] in existing_ids:
            self.dup.inc()           # already present → skip
        else:
            self.write.inc()
            yield element

# ───────────── Pipeline ─────────────
def run(argv=None):
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

    options = PipelineOptions(argv)
    gco = options.view_as(GoogleCloudOptions)
    gco.project  = PROJECT_ID
    gco.region   = REGION
    gco.job_name = f'delta-load-parse-country-ods-pipeline-{timestamp}'
    gco.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    gco.temp_location    = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:

        # --- US-states lookup table → dict side-input -----------------
        states_dict = AsDict(
            p
            | 'Read US States' >> ReadFromBigQuery(
                table=f'imp-fyp.ODS_ds.50-US-States',
                use_standard_sql=True)
            | 'States KV' >> beam.FlatMap(
                lambda r: [
                    (r['State'].strip(),        r['State']),
                    (r['Abbreviation'].strip(), r['State'])
                ])
        )

        # --- snapshot existing ID_Event keys from OUTPUT table -------
        existing_ids = (
            p
            | 'Read Existing IDs' >> ReadFromBigQuery(
                query=f"SELECT ID_Event FROM `{PROJECT_ID}.{DATASET}.{OUTPUT_TABLE}`",
                use_standard_sql=True)
            | 'IDs to list' >> beam.Map(lambda r: r['ID_Event'])
        )
        id_side = beam.pvalue.AsList(existing_ids)

        # --- main flow ----------------------------------------------
        (
            p
            | 'Read INPUT ODS' >> ReadFromBigQuery(
                table=f'{PROJECT_ID}:{DATASET}.{INPUT_TABLE}',
                use_standard_sql=True)
            | 'Parse Country'  >> beam.ParDo(ParsePlaceDoFn(), states_dict)
            | 'Attach JobId'   >> beam.ParDo(_AttachJobId())
            | 'Filter Dups'    >> beam.ParDo(DropExistingIds(), id_side)

            | 'Write OUTPUT' >> WriteToBigQuery(
                table=f'{PROJECT_ID}:{DATASET}.{OUTPUT_TABLE}',
                schema=None,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

    logging.info("Pipeline finished.")

if __name__ == '__main__':
    run()
