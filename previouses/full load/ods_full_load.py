import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime, timedelta, timezone
import hashlib
import os                                                 
from apache_beam.io.gcp import gce_metadata_util          
# ───────────── CONFIGURATION ─────────────
PROJECT_ID = 'imp-fyp'
REGION = 'us-central1'
STAGING_DATASET = 'STG_ds'
ODS_DATASET = 'ODS_ds'
STAGING_TABLE = 'T_STG_day_earthquake'
ODS_TABLE = 'ods_day_earthquake'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'
EEST = timezone(timedelta(hours=3))
timestamp = datetime.now(EEST).strftime('%Y%m%d-%H%M%S')
# ─────────────────────────────────────────

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
        

def to_eest_datetime_str(utc_str):
    try:
        utc = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        eest_time = utc + timedelta(hours=3)
        return eest_time.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

class TransformToODS(beam.DoFn):
    def __init__(self, job_timestamp):
        self.job_timestamp = job_timestamp

    def setup(self):
        self.updated_date = datetime.now(EEST).strftime("%Y-%m-%d %H:%M:%S")

    def from_yyyymmdd_hhmmss(self, val):
        try:
            return datetime.strptime(val, "%Y%m%d-%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None

    def to_float(self, val):
        try:
            return float(val) if val not in [None, '', 'null'] else None
        except:
            return None

    def to_int(self, val):
        try:
            return int(float(val)) if val not in [None, '', 'null'] else None
        except:
            return None

    def clean(self, val):
        return val if val not in [None, '', 'null'] else None

    def process(self, row):
        try:
            lat = self.to_float(row.get('latitude'))
            lon = self.to_float(row.get('longitude'))
            depth = self.to_float(row.get('depth'))
            depth_error = self.to_float(row.get('depthError'))
            mag = self.to_float(row.get('mag'))
            mag_error = self.to_float(row.get('magError'))

            vl_n_depth = depth + 0.5 * depth_error if (depth is not None and depth_error is not None) else depth
            vl_n_mag = mag + 0.5 * mag_error if (mag is not None and mag_error is not None) else mag

            # Depth category
            depth_cat = None
            if vl_n_depth is not None:
                if vl_n_depth <= 70:
                    depth_cat = "Shallow"
                elif 70 < vl_n_depth <= 300:
                    depth_cat = "Intermediate"
                else:
                    depth_cat = "Deep"

            # Magnitude category
            mag_cat = None
            if vl_n_mag is not None:
                if vl_n_mag < 3:
                    mag_cat = "Not Felt"
                elif 3 <= vl_n_mag < 4:
                    mag_cat = "Minor"
                elif 4 <= vl_n_mag < 5:
                    mag_cat = "Light"
                elif 5 <= vl_n_mag < 6:
                    mag_cat = "Moderate"
                elif 6 <= vl_n_mag < 7:
                    mag_cat = "Strong"
                elif 7 <= vl_n_mag < 8:
                    mag_cat = "Major"
                else:
                    mag_cat = "Great"

            dt_time = to_eest_datetime_str(row.get('time'))
            insertion_dt = self.from_yyyymmdd_hhmmss(row.get('insertion_date'))

            yield {
                'ID_Event': abs(hash(f"{row.get('time')}{lat}{lon}")),
                'VL_n_mag': vl_n_mag,
                'LB_magCategory': mag_cat,
                'VL_n_depth': vl_n_depth,
                'LB_depthCategory': depth_cat,
                'LB_Region': None,
                'LB_Country': None,
                'LB_place': self.clean(row.get('place')),
                'DT_time': dt_time,
                'VL_latitude': lat,
                'VL_longitude': lon,
                'ID_nst': self.to_int(row.get('nst')),
                'ID_gap': self.to_int(row.get('gap')),
                'VL_dmin': self.to_float(row.get('dmin')),
                'LB_net': self.clean(row.get('net')),
                'LB_type': self.clean(row.get('type')),
                'VL_horizontalError': self.to_float(row.get('horizontalError')),
                'ID_magNst': self.to_int(row.get('magNst')),
                'LB_status': self.clean(row.get('status')),
                'LB_locationSource': self.clean(row.get('locationSource')),
                'LB_magSource': self.clean(row.get('magSource')),
                '_DT_insertion_date': insertion_dt,
                '_DT_updated_date': self.updated_date,
                '_LB_job_execution_id': self.clean(row.get('job_execution_id')),
                '_LB_data_source': self.clean(row.get('data_source'))
            }

        except Exception as e:
            print(f"[ERROR] Skipping row of type={row.get('type')}, error={e}, row={row}")

def run():
    options = PipelineOptions()
    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    gco.job_name = f'ods-full-load-{timestamp.lower()}'
    gco.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    gco.temp_location = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from Staging BQ' >> beam.io.ReadFromBigQuery(
                table=f'{PROJECT_ID}:{STAGING_DATASET}.{STAGING_TABLE}',
                use_standard_sql=True
            )
            | 'Transform Records' >> beam.ParDo(TransformToODS(timestamp))
            | 'Attach Job-ID'     >> beam.ParDo(_AttachJobId())
            | 'Write to ODS BQ' >> beam.io.WriteToBigQuery(
                f'{PROJECT_ID}:{ODS_DATASET}.{ODS_TABLE}',
                schema=None,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE
            )
        )

if __name__ == '__main__':
    run()
