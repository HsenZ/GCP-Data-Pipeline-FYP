import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
from apache_beam.metrics import Metrics
from apache_beam.io.gcp import gce_metadata_util
from dateutil import parser 
from datetime import datetime, timedelta, timezone
import pytz, os, hashlib, logging

# ─── constants ────────────────────────────────────────────────────
PROJECT_ID       = 'imp-fyp'
STAGING_DATASET  = 'STG_ds'
STAGING_TABLE    = 'T_STG_day_earthquake'
ODS_DATASET      = 'ODS_ds'
ODS_TABLE        = 'ods_day_earthquake'        
EEST             = pytz.timezone('Europe/Bucharest')

MASK63 = 0x7FFFFFFFFFFFFFFF      
is_updated = False

def stable_id(time_str, lat_raw, lon_raw):
   # SHA-1 HASH
    key = f"{time_str}_{lat_raw}_{lon_raw}".encode("utf-8")
    return int(hashlib.sha1(key).hexdigest()[:16], 16) & MASK63

def to_eest_datetime_str(iso_ts):
   # timezone convertion
    try:
        dt = parser.isoparse(iso_ts)           
        dt = dt.astimezone(EEST)               
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

# job-ID DoFn
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

# Transform from staging schema to ODS row 
class TransformToODS(beam.DoFn):
    def __init__(self, job_timestamp):
        self.job_timestamp = job_timestamp
        self.rows_in = Metrics.counter('ods', 'rows_in')  # metric

    def setup(self):
        self.updated_date = datetime.now(EEST).strftime("%Y-%m-%d %H:%M:%S")

    # helper converters
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
            # keep raw strings for deterministic ID
            lat_raw = row.get('latitude')
            lon_raw = row.get('longitude')
            lat = self.to_float(lat_raw)
            lon = self.to_float(lon_raw)

            depth        = self.to_float(row.get('depth'))
            depth_error  = self.to_float(row.get('depthError'))
            mag          = self.to_float(row.get('mag'))
            mag_error    = self.to_float(row.get('magError'))

            vl_n_depth = depth + 0.5*depth_error if depth is not None and depth_error is not None else depth
            vl_n_mag   = mag   + 0.5*mag_error   if mag   is not None and mag_error   is not None else mag

            # categories
            depth_cat = None
            if vl_n_depth is not None:
                depth_cat = "Shallow" if vl_n_depth <= 70 else "Intermediate" if vl_n_depth <= 300 else "Deep"
            mag_cat = None
            if vl_n_mag is not None:
                if   vl_n_mag < 3: mag_cat = "Not Felt"
                elif vl_n_mag < 4: mag_cat = "Minor"
                elif vl_n_mag < 5: mag_cat = "Light"
                elif vl_n_mag < 6: mag_cat = "Moderate"
                elif vl_n_mag < 7: mag_cat = "Strong"
                elif vl_n_mag < 8: mag_cat = "Major"
                else:              mag_cat = "Great"

            # time fields (FIXED: add dt_time)
            dt_time = to_eest_datetime_str(row.get('time'))
            eest = pytz.timezone('Europe/Bucharest')
            updated_time = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)
            insertion_date = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

            self.rows_in.inc() 
            yield {
                'ID_Event'           : stable_id(dt_time, lat_raw, lon_raw),    
                'VL_n_mag'           : vl_n_mag,
                'LB_magCategory'     : mag_cat,
                'VL_n_depth'         : vl_n_depth,
                'LB_depthCategory'   : depth_cat,
                'LB_Region'          : None,
                'LB_Country'         : None,
                'LB_place'           : self.clean(row.get('place')),
                'DT_time'            : dt_time,
                'VL_latitude'        : lat,
                'VL_longitude'       : lon,
                'ID_nst'             : self.to_int(row.get('nst')),
                'ID_gap'             : self.to_int(row.get('gap')),
                'VL_dmin'            : self.to_float(row.get('dmin')),
                'LB_net'             : self.clean(row.get('net')),
                'LB_type'            : self.clean(row.get('type')),
                'VL_horizontalError' : self.to_float(row.get('horizontalError')),
                'ID_magNst'          : self.to_int(row.get('magNst')),
                'LB_status'          : self.clean(row.get('status')),
                'LB_locationSource'  : self.clean(row.get('locationSource')),
                'LB_magSource'       : self.clean(row.get('magSource')),
                '_DT_insertion_date' : insertion_date,
                '_DT_updated_date'   : updated_time if is_updated else None,
                '_LB_job_execution_id': self.clean(row.get('job_execution_id')), 
                '_LB_data_source'    : self.clean(row.get('data_source'))
            }
        except Exception as e:
            logging.warning("Skipping row due to error: %s", e)

#  Deduplicate step
class DeduplicateById(beam.DoFn):
    def __init__(self):
        self.dup   = Metrics.counter('ods', 'rows_filtered')
        self.writt = Metrics.counter('ods', 'rows_written')

    def process(self, element, existing_ids):
        if element['ID_Event'] in existing_ids:
            self.dup.inc()          # drop duplicate
        else:
            self.writt.inc()
            yield element

#  main pipeline 
def run(argv=None):
    EEST = timezone(timedelta(hours=3))
    print(f"Pipeline execution STARTED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")
    ts = datetime.now(EEST).strftime('%Y%m%d-%H%M%S')

    options = PipelineOptions(argv)
    gco = options.view_as(GoogleCloudOptions)
    gco.project  = PROJECT_ID
    gco.job_name = f'delta-load-ods-pipeline-{ts}'

    with beam.Pipeline(options=options) as p:

        # Snapshot existing keys once
        existing_ids = (
            p
            | 'Read Existing IDs' >> ReadFromBigQuery(
                query=f"SELECT ID_Event FROM `{PROJECT_ID}.{ODS_DATASET}.{ODS_TABLE}`",
                use_standard_sql=True)
            | 'Only ID' >> beam.Map(lambda r: r['ID_Event'])
        )
        id_side = beam.pvalue.AsList(existing_ids)

        # Main flow
        (
            p
            | 'Read Staging' >> ReadFromBigQuery(
                table=f'{PROJECT_ID}:{STAGING_DATASET}.{STAGING_TABLE}',
                use_standard_sql=True
            )
            | 'Transform' >> beam.ParDo(TransformToODS(ts))
            | 'Attach JobID' >> beam.ParDo(_AttachJobId())
            | 'Deduplicate'  >> beam.ParDo(DeduplicateById(), id_side)
            | 'Write ODS' >> WriteToBigQuery(
                f'{PROJECT_ID}:{ODS_DATASET}.{ODS_TABLE}',
                schema=None,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

    print(f"Pipeline FINISHED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")

if __name__ == '__main__':
    run()
