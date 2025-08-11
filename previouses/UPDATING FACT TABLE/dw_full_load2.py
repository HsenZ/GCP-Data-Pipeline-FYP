import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, ReadFromBigQuery, BigQueryDisposition
from datetime import datetime
import pytz
from dateutil import parser
import logging
import os
from apache_beam.io.gcp import gce_metadata_util    # NEW


PROJECT_ID = 'imp-fyp'
ODS_DATASET = 'ODS_ds'
DW_DATASET = 'DW_ds'
REGION = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

is_updated = False 

class _AttachJobId(beam.DoFn):
    """Adds the current Dataflow job-ID to each element."""
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

def assign_ids(elements):
    for idx, value in enumerate(sorted(elements), start=1):
        yield (value, idx)

def format_date_dim(date_str):
    try:
        dt = parser.parse(date_str).date()
        return {
            'ID_date_ID': int(dt.strftime('%Y%m%d')),
            'DT_date': dt.strftime('%Y-%m-%d'),
            'LB_DayOfWeek': dt.strftime('%A'),
            'ID_DayOfWeekNb': dt.isoweekday(),
            'ID_WeekNb': dt.isocalendar()[1],
            'LB_MonthName': dt.strftime('%B'),
            'ID_MonthNb': dt.month,
            'ID_Quarter': (dt.month - 1) // 3 + 1,
            'ID_Year': dt.year
        }
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return None

def get_depth_description(name):
    return {
        'Shallow': 'less than 70km',
        'Intermediate': 'between 70km and 300km',
        'Deep': 'more than 300km'
    }.get(name, f"Depth category: {name}")

def get_mag_description(name):
    return {
        'Not Felt': '<3',
        'Minor': '3 <=mag < 4',
        'Light': '4 <=mag < 5',
        'Moderate': '5 <=mag < 6',
        'Strong': '6 <= mag < 7',
        'Major': '7 <=mag < 8',
        'Great': '8 <= mag'
    }.get(name, f"Magnitude category: {name}")

def run(argv=None):
    options = PipelineOptions(argv)
    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    eest = pytz.timezone('Europe/Bucharest')
    timestamp_str = datetime.now(pytz.utc).astimezone(eest).strftime('%Y%m%d-%H%M%S')
    gco.job_name = f'dw-full-load-{timestamp_str}'

    gco.staging_location = f'gs://{INTERMEDIATE_BUCKET}/staging'
    gco.temp_location = f'gs://{INTERMEDIATE_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(SetupOptions).save_main_session = True

    # Set actual job execution ID
    job_execution_id = os.environ.get("DATAFLOW_JOB_ID") or os.environ.get("JOB_ID") or "LOCAL_RUN"

    with beam.Pipeline(options=options) as p:
        ods_rows = p | 'ReadODS' >> ReadFromBigQuery(
            table=f'{PROJECT_ID}:{ODS_DATASET}.T_ODS_day_earthquake',
            use_standard_sql=True)

        # Network Dimension
        network_kvs = (
            ods_rows | 'ExtractNetwork' >> beam.Map(lambda row: row['LB_net'])
                      | 'DistinctNetwork' >> beam.Distinct()
                      | 'ToListNetwork' >> beam.combiners.ToList()
                      | 'AssignNetworkIDs' >> beam.FlatMap(assign_ids)
        )
        network_kvs | 'WriteNetworkToBQ' >> beam.Map(lambda kv: {
            'ID_Network_ID': kv[1], 'LB_NetworkSymbol': kv[0]
        }) | 'WriteNetworkDim' >> WriteToBigQuery(
            f'{PROJECT_ID}:{DW_DATASET}.T_DIM_Network',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        # RegionCountry Dimension
        region_country_kvs = (
            ods_rows | 'ExtractRegionCountry' >> beam.Map(lambda row: (row['LB_Region'], row['LB_Country']))
                      | 'DistinctRegionCountry' >> beam.Distinct()
                      | 'ToListRegionCountry' >> beam.combiners.ToList()
                      | 'AssignRegionCountryIDs' >> beam.FlatMap(assign_ids)
        )
        region_country_kvs | 'WriteRegionToBQ' >> beam.Map(lambda kv: {
            'ID_RegionCountry_ID': kv[1], 'LB_Region': kv[0][0], 'LB_Country': kv[0][1]
        }) | 'WriteRegionCountryDim' >> WriteToBigQuery(
            f'{PROJECT_ID}:{DW_DATASET}.T_DIM_RegionCountry',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        # Type Dimension
        type_kvs = (
            ods_rows | 'ExtractType' >> beam.Map(lambda row: row['LB_type'])
                      | 'DistinctType' >> beam.Distinct()
                      | 'ToListType' >> beam.combiners.ToList()
                      | 'AssignTypeIDs' >> beam.FlatMap(assign_ids)
        )
        type_kvs | 'WriteTypeToBQ' >> beam.Map(lambda kv: {
            'ID_type_ID': kv[1], 'LB_type': kv[0]
        }) | 'WriteTypeDim' >> WriteToBigQuery(
            f'{PROJECT_ID}:{DW_DATASET}.T_DIM_Seismic_Activity_Type',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        # Mag Category Dimension
        mag_kvs = (
            ods_rows | 'ExtractMagCategory' >> beam.Map(lambda row: row['LB_magCategory'] or 'Not Felt')
                      | 'DistinctMagCategory' >> beam.Distinct()
                      | 'ToListMag' >> beam.combiners.ToList()
                      | 'AssignMagIDs' >> beam.FlatMap(assign_ids)
        )
        mag_kvs | 'WriteMagToBQ' >> beam.Map(lambda kv: {
            'ID_magCategory_ID': kv[1], 'LB_magCategoryName': kv[0], 'LB_magCategoryDescription': get_mag_description(kv[0])
        }) | 'WriteMagDim' >> WriteToBigQuery(
            f'{PROJECT_ID}:{DW_DATASET}.T_DIM_magCategory',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        # Depth Category Dimension
        depth_kvs = (
            ods_rows | 'ExtractDepthCategory' >> beam.Map(lambda row: row['LB_depthCategory'] or 'Shallow')
                      | 'DistinctDepthCategory' >> beam.Distinct()
                      | 'ToListDepth' >> beam.combiners.ToList()
                      | 'AssignDepthIDs' >> beam.FlatMap(assign_ids)
        )
        depth_kvs | 'WriteDepthToBQ' >> beam.Map(lambda kv: {
            'ID_depthCategory_ID': kv[1], 'LB_depthCategoryName': kv[0], 'LB_depthCategoryDescription': get_depth_description(kv[0])
        }) | 'WriteDepthDim' >> WriteToBigQuery(
            f'{PROJECT_ID}:{DW_DATASET}.T_DIM_depthCategory',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        # Date Dimension
        date_dim = (
            ods_rows
            | 'ExtractDatePart' >> beam.Map(
                lambda row: parser.parse(row['DT_time']).strftime('%Y-%m-%d'))
            | 'DistinctDatePart' >> beam.Distinct()
            | 'FormatDates' >> beam.Map(format_date_dim)
            | 'WriteDateDim' >> WriteToBigQuery(
                f'{PROJECT_ID}:{DW_DATASET}.T_DIM_date',
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE)
        )

        # FACT TABLE
        def enrich_fact(row, net_map, reg_map, type_map, mag_map, depth_map):
            eest = pytz.timezone('Europe/Bucharest')
            updated_time = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

            dt_obj = parser.parse(row['DT_time']).replace(tzinfo=None) if isinstance(row['DT_time'], str) else row['DT_time'].replace(tzinfo=None)
            date_id = int(dt_obj.strftime('%Y%m%d'))


            insertion_date = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

            return {
                'ID_Event': row['ID_Event'],
                'ID_Network_ID': net_map.get(row['LB_net']),
                'ID_RegionCountry_ID': reg_map.get((row['LB_Region'], row['LB_Country'])),
                'ID_type_ID': type_map.get(row['LB_type']),
                'ID_date_ID': date_id,
                'ID_depthCategory_ID': depth_map.get(row['LB_depthCategory'] or 'Shallow'),
                'ID_magCategory_ID': mag_map.get(row['LB_magCategory'] or 'Not Felt'),
                'VL_n_mag': float(row['VL_n_mag']) if row['VL_n_mag'] is not None else None,
                # 'LB_magCategory': row['LB_magCategory'],
                'VL_n_depth': float(row['VL_n_depth']) if row['VL_n_depth'] is not None else None,
                # 'LB_depthCategory': row['LB_depthCategory'],
                # 'LB_Region': row['LB_Region'],
                # 'LB_Country': row['LB_Country'],
                'LB_place': row['LB_place'],
                'DT_time': dt_obj,
                'VL_latitude': float(row['VL_latitude']) if row['VL_latitude'] is not None else None,
                'VL_longitude': float(row['VL_longitude']) if row['VL_longitude'] is not None else None,
                'ID_nst': int(row['ID_nst']) if row['ID_nst'] is not None else None,
                'ID_gap': int(row['ID_gap']) if row['ID_gap'] is not None else None,
                'VL_dmin': float(row['VL_dmin']) if row['VL_dmin'] is not None else None,
                # 'LB_net': row['LB_net'],
                # 'LB_type': row['LB_type'],
                'VL_horizontalError': float(row['VL_horizontalError']) if row['VL_horizontalError'] is not None else None,
                'ID_magNst': int(row['ID_magNst']) if row['ID_magNst'] is not None else None,
                # 'LB_status': row['LB_status'],
                # 'LB_locationSource': row['LB_locationSource'],
                # 'LB_magSource': row['LB_magSource'],
                '_DT_insertion_date': row['_DT_insertion_date'] if is_updated else insertion_date,
                '_DT_updated_date': updated_time if is_updated else None ,
                '_LB_job_execution_id': job_execution_id,
                '_LB_data_source': row.get('_LB_data_source', 'T_ODS_day_earthquake')
            }

        fact_records = (
            ods_rows | 'EnrichFactRows' >> beam.Map(
                enrich_fact,
                net_map=beam.pvalue.AsDict(network_kvs),
                reg_map=beam.pvalue.AsDict(region_country_kvs),
                type_map=beam.pvalue.AsDict(type_kvs),
                mag_map=beam.pvalue.AsDict(mag_kvs),
                depth_map=beam.pvalue.AsDict(depth_kvs)
            )
            | 'AttachJobId' >> beam.ParDo(_AttachJobId())
        )

        fact_records | 'WriteFactTable' >> WriteToBigQuery(
            table=f'{PROJECT_ID}:{DW_DATASET}.T_FACT_Events',
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            method='FILE_LOADS',
            schema=None,
            custom_gcs_temp_location=f'gs://{INTERMEDIATE_BUCKET}/temp')

if __name__ == '__main__':
    run()
