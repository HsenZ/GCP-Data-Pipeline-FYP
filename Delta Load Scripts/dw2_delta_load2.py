import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
)
from apache_beam.io.gcp.bigquery import (
    ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
)
from apache_beam.transforms.util import Flatten
import datetime as dt          
from datetime import datetime, timedelta, timezone
import pytz
from dateutil import parser
import logging, os

from apache_beam.io.gcp import gce_metadata_util
from google.cloud import bigquery

from google.cloud.exceptions import NotFound

# ────────── CONSTANTS ──────────────────────────────────────────────
PROJECT_ID          = 'imp-fyp'
ODS_DATASET         = 'ODS_ds'
DW_DATASET          = 'DW_ds'
REGION              = 'us-central1'
INTERMEDIATE_BUCKET = 'dataflow-intermediate-bucket'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

is_updated = False
 
def assign_ids(elements, start_id=1):
    for idx, val in enumerate(sorted(elements), start=start_id):
        yield (val, idx)

def format_date_dim(date_str: str):
    dt_local = parser.parse(date_str).date()
    return {
        'ID_date_ID'     : int(dt_local.strftime('%Y%m%d')),
        'DT_date'        : dt_local.strftime('%Y-%m-%d'),
        'LB_DayOfWeek'   : dt_local.strftime('%A'),
        'ID_DayOfWeekNb' : dt_local.isoweekday(),
        'ID_WeekNb'      : dt_local.isocalendar()[1],
        'LB_MonthName'   : dt_local.strftime('%B'),
        'ID_MonthNb'     : dt_local.month,
        'ID_Quarter'     : (dt_local.month - 1)//3 + 1,
        'ID_Year'        : dt_local.year
    }

def get_depth_description(name):
    return {
        'Shallow'    : 'less than 70km',
        'Intermediate': 'between 70km and 300km',
        'Deep'       : 'more than 300km'
    }.get(name, f"Depth category: {name}")

def get_mag_description(name):
    return {
        'Not Felt': '<3',
        'Minor'   : '3 ≤ mag < 4',
        'Light'   : '4 ≤ mag < 5',
        'Moderate': '5 ≤ mag < 6',
        'Strong'  : '6 ≤ mag < 7',
        'Major'   : '7 ≤ mag < 8',
        'Great'   : '8 ≤ mag'
    }.get(name, f"Magnitude category: {name}")

#  BigQuery Surrogate key assihnement, current max surrogate-key 
bq_client = bigquery.Client(project=PROJECT_ID)
def _get_max_id(table_name, id_col):
    sql = f"SELECT MAX({id_col}) AS max_id FROM `{PROJECT_ID}.{DW_DATASET}.{table_name}`"
    row = next(bq_client.query(sql).result(), None)
    return row.max_id or 0 if row else 0

def _ensure_staging_table():
    main_tbl_id = f"{PROJECT_ID}.{DW_DATASET}.T_FACT_Events"
    stage_tbl_id = f"{PROJECT_ID}.{DW_DATASET}.T_FACT_Events_staging"
    try:
        bq_client.get_table(stage_tbl_id)             
    except NotFound:
        main_tbl = bq_client.get_table(main_tbl_id)
        table = bigquery.Table(stage_tbl_id, schema=main_tbl.schema)
        bq_client.create_table(table)
        logging.info("Created staging table with cloned schema.")

#  DoFn to attach Dataflow job-ID 
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

#  One-shot MERGE for the Fact table 
def _merge_fact_table():
    merge_sql = f"""
    MERGE `{PROJECT_ID}.{DW_DATASET}.T_FACT_Events` T
    USING `{PROJECT_ID}.{DW_DATASET}.T_FACT_Events_staging` S
    ON  T.ID_Event = S.ID_Event
    WHEN MATCHED THEN UPDATE SET
      ID_Network_ID       = S.ID_Network_ID,
      ID_RegionCountry_ID = S.ID_RegionCountry_ID,
      ID_type_ID          = S.ID_type_ID,
      ID_date_ID          = S.ID_date_ID,
      ID_depthCategory_ID = S.ID_depthCategory_ID,
      ID_magCategory_ID   = S.ID_magCategory_ID,
      VL_n_mag            = S.VL_n_mag,
      VL_n_depth          = S.VL_n_depth,
      LB_place            = S.LB_place,
      DT_time             = S.DT_time,
      VL_latitude         = S.VL_latitude,
      VL_longitude        = S.VL_longitude,
      ID_nst              = S.ID_nst,
      ID_gap              = S.ID_gap,
      VL_dmin             = S.VL_dmin,
      VL_horizontalError  = S.VL_horizontalError,
      ID_magNst           = S.ID_magNst,
      _DT_insertion_date  = S._DT_insertion_date,
      _DT_updated_date    = S._DT_updated_date,
      _LB_job_execution_id= S._LB_job_execution_id,
      _LB_data_source     = S._LB_data_source
    WHEN NOT MATCHED THEN INSERT ROW
    """
    bq_client.query(merge_sql).result()
    logging.info("MERGE into T_FACT_Events finished.")

#  PIPELINE 
def run(argv=None):
    _ensure_staging_table()
    EEST = timezone(timedelta(hours=3))
    print(f"Pipeline execution STARTED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")
    #  snapshot current surrogate-key maxima
    max_network_id        = _get_max_id("T_DIM_Network",           "ID_Network_ID")
    max_region_country_id = _get_max_id("T_DIM_RegionCountry",     "ID_RegionCountry_ID")
    max_type_id           = _get_max_id("T_DIM_Seismic_Activity_Type", "ID_type_ID")
    max_mag_id            = _get_max_id("T_DIM_magCategory",       "ID_magCategory_ID")
    max_depth_id          = _get_max_id("T_DIM_depthCategory",     "ID_depthCategory_ID")

    #  Beam / Dataflow options
    options = PipelineOptions(argv)
    gco = options.view_as(GoogleCloudOptions)
    gco.project  = PROJECT_ID
    gco.region   = REGION
    gco.job_name = f"delta-load-dw-pipeline-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    gco.staging_location = f"gs://{INTERMEDIATE_BUCKET}/staging"
    gco.temp_location    = f"gs://{INTERMEDIATE_BUCKET}/temp"
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)

    #  READ ODS 
    ods_rows = p | 'ReadODS' >> ReadFromBigQuery(
        table=f'{PROJECT_ID}:{ODS_DATASET}.T_ODS_day_earthquake',
        use_standard_sql=True)

    #  DIMENSION: Network 
    existing_net = (
        p
        | 'ReadDimNetwork' >> ReadFromBigQuery(
            query=f"SELECT LB_NetworkSymbol, ID_Network_ID FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_Network`",
            use_standard_sql=True)
    )
    existing_net_kv = existing_net | 'KV_ExistingNet' >> beam.Map(lambda r: (r['LB_NetworkSymbol'], r['ID_Network_ID']))
    net_dict = beam.pvalue.AsDict(existing_net_kv)

    new_net_kv = (
        ods_rows
        | 'ExtractNetwork' >> beam.Map(lambda r: r['LB_net'])
        | 'DistinctNetworkVal' >> beam.Distinct()
        | 'FilterNewNet' >> beam.Filter(lambda n, d: n not in d, net_dict)
        | 'NetToList' >> beam.combiners.ToList()
        | 'AssignNetIDs' >> beam.FlatMap(lambda lst, start=max_network_id+1: assign_ids(lst, start))
    )

    new_net_kv | 'WriteNet' >> (
        beam.Map(lambda kv: {'ID_Network_ID': kv[1], 'LB_NetworkSymbol': kv[0]})
        | WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_Network',
                            schema=None,
                            create_disposition=BigQueryDisposition.CREATE_NEVER,
                            write_disposition=BigQueryDisposition.WRITE_APPEND))

    all_net_kv = (existing_net_kv, new_net_kv) | 'AllNetKV' >> Flatten()
    full_net_map = beam.pvalue.AsDict(all_net_kv)

    #  DIMENSION: Region × Country 
    existing_reg = (
        p | 'ReadDimReg' >> ReadFromBigQuery(
            query=f"SELECT LB_Region, LB_Country, ID_RegionCountry_ID FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_RegionCountry`",
            use_standard_sql=True)
    )
    existing_reg_kv = existing_reg | beam.Map(lambda r: ((r['LB_Region'], r['LB_Country']), r['ID_RegionCountry_ID']))
    reg_dict = beam.pvalue.AsDict(existing_reg_kv)

    new_reg_kv = (
        ods_rows
        | 'ExtractReg' >> beam.Map(lambda r: (r['LB_Region'], r['LB_Country']))
        | 'DistinctRegVal' >> beam.Distinct()
        | 'FilterNewReg' >> beam.Filter(lambda rc, d: rc not in d, reg_dict)
        | 'RegToList' >> beam.combiners.ToList()
        | 'AssignRegIDs' >> beam.FlatMap(lambda lst, start=max_region_country_id+1: assign_ids(lst, start))
    )

    new_reg_kv | 'WriteReg' >> (
        beam.Map(lambda kv: {
            'ID_RegionCountry_ID': kv[1],
            'LB_Region'          : kv[0][0],
            'LB_Country'         : kv[0][1]
        })
        | WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_RegionCountry',
                            schema =None,
                            create_disposition=BigQueryDisposition.CREATE_NEVER,
                            write_disposition=BigQueryDisposition.WRITE_APPEND))

    all_reg_kv = (existing_reg_kv, new_reg_kv) | 'AllRegKV' >> Flatten()
    full_reg_map = beam.pvalue.AsDict(all_reg_kv)

    #  DIMENSION: Type 
    existing_type = (
        p | 'ReadDimType' >> ReadFromBigQuery(
            query=f"SELECT LB_type, ID_type_ID FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_Seismic_Activity_Type`",
            use_standard_sql=True)
    )
    existing_type_kv = existing_type | beam.Map(lambda r: (r['LB_type'], r['ID_type_ID']))
    type_dict = beam.pvalue.AsDict(existing_type_kv)

    new_type_kv = (
        ods_rows
        | 'ExtractType' >> beam.Map(lambda r: r['LB_type'])
        | 'DistinctTypeVal' >> beam.Distinct()
        | 'FilterNewType' >> beam.Filter(lambda t, d: t not in d, type_dict)
        | 'TypeToList' >> beam.combiners.ToList()
        | 'AssignTypeIDs' >> beam.FlatMap(lambda lst, start=max_type_id+1: assign_ids(lst, start))
    )

    new_type_kv | 'WriteType' >> (
        beam.Map(lambda kv: {'ID_type_ID': kv[1], 'LB_type': kv[0]})
        | WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_Seismic_Activity_Type',
                            schema=None,
                            create_disposition=BigQueryDisposition.CREATE_NEVER,
                            write_disposition=BigQueryDisposition.WRITE_APPEND))

    all_type_kv = (existing_type_kv, new_type_kv) | 'AllTypeKV' >> Flatten()
    full_type_map = beam.pvalue.AsDict(all_type_kv)

    #  DIMENSION: magCategory 
    existing_mag = (
        p | 'ReadDimMag' >> ReadFromBigQuery(
            query=f"SELECT LB_magCategoryName, ID_magCategory_ID FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_magCategory`",
            use_standard_sql=True)
    )
    existing_mag_kv = existing_mag | beam.Map(lambda r: (r['LB_magCategoryName'], r['ID_magCategory_ID']))
    mag_dict = beam.pvalue.AsDict(existing_mag_kv)

    new_mag_kv = (
        ods_rows
        | 'ExtractMag' >> beam.Map(lambda r: r['LB_magCategory'] or 'Not Felt')
        | 'DistinctMagVal' >> beam.Distinct()
        | 'FilterNewMag' >> beam.Filter(lambda m, d: m not in d, mag_dict)
        | 'MagToList' >> beam.combiners.ToList()
        | 'AssignMagIDs' >> beam.FlatMap(lambda lst, start=max_mag_id+1: assign_ids(lst, start))
    )

    new_mag_kv | 'WriteMag' >> (
        beam.Map(lambda kv: {
            'ID_magCategory_ID'        : kv[1],
            'LB_magCategoryName'       : kv[0],
            'LB_magCategoryDescription': get_mag_description(kv[0])
        })
        | WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_magCategory',
                            schema= None,
                            create_disposition=BigQueryDisposition.CREATE_NEVER,
                            write_disposition=BigQueryDisposition.WRITE_APPEND))

    all_mag_kv = (existing_mag_kv, new_mag_kv) | 'AllMagKV' >> Flatten()
    full_mag_map = beam.pvalue.AsDict(all_mag_kv)

    #  DIMENSION: depthCategory 
    existing_depth = (
        p | 'ReadDimDepth' >> ReadFromBigQuery(
            query=f"SELECT LB_depthCategoryName, ID_depthCategory_ID FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_depthCategory`",
            use_standard_sql=True)
    )
    existing_depth_kv = existing_depth | beam.Map(lambda r: (r['LB_depthCategoryName'], r['ID_depthCategory_ID']))
    depth_dict = beam.pvalue.AsDict(existing_depth_kv)

    new_depth_kv = (
        ods_rows
        | 'ExtractDepth' >> beam.Map(lambda r: r['LB_depthCategory'] or 'Shallow')
        | 'DistinctDepthVal' >> beam.Distinct()
        | 'FilterNewDepth' >> beam.Filter(lambda d, s: d not in s, depth_dict)
        | 'DepthToList' >> beam.combiners.ToList()
        | 'AssignDepthIDs' >> beam.FlatMap(lambda lst, start=max_depth_id+1: assign_ids(lst, start))
    )

    new_depth_kv | 'WriteDepth' >> (
        beam.Map(lambda kv: {
            'ID_depthCategory_ID'        : kv[1],
            'LB_depthCategoryName'       : kv[0],
            'LB_depthCategoryDescription': get_depth_description(kv[0])
        })
        | WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_depthCategory',
                            schema = None,
                            create_disposition=BigQueryDisposition.CREATE_NEVER,
                            write_disposition=BigQueryDisposition.WRITE_APPEND))

    all_depth_kv = (existing_depth_kv, new_depth_kv) | 'AllDepthKV' >> Flatten()
    full_depth_map = beam.pvalue.AsDict(all_depth_kv)

    #  DIMENSION: Date (with AsList) 
    existing_dates = (
        p | 'ReadDimDate' >> ReadFromBigQuery(
            query=f"SELECT DT_date FROM `{PROJECT_ID}.{DW_DATASET}.T_DIM_date`",
            use_standard_sql=True)
    )
    date_list = beam.pvalue.AsList(
        existing_dates
        | 'MapDT' >> beam.Map(lambda r: r['DT_date'].strftime('%Y-%m-%d')   # cast to str
                            if hasattr(r['DT_date'], 'strftime')
                            else str(r['DT_date']))
    )

    (
        ods_rows
        | 'ExtractDatePart' >> beam.Map(lambda r: parser.parse(r['DT_time']).strftime('%Y-%m-%d'))
        | 'DistinctDateVal' >> beam.Distinct()
        | 'FilterNewDate'   >> beam.Filter(lambda d, lst: d not in set(lst), date_list)
        | 'FormatDateRows'  >> beam.Map(format_date_dim)
        | 'WriteDate'       >> WriteToBigQuery(f'{PROJECT_ID}:{DW_DATASET}.T_DIM_date',
                                                schema = None,
                                                create_disposition=BigQueryDisposition.CREATE_NEVER,
                                                write_disposition=BigQueryDisposition.WRITE_APPEND)
    )

    #  FACT TABLE  (to staging) 
    def enrich_fact(row, net_map, reg_map, type_map, mag_map, depth_map):
        eest = pytz.timezone('Europe/Bucharest')
        updated_time = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

        dt_obj = parser.parse(row['DT_time']) if isinstance(row['DT_time'], str) else row['DT_time']
        dt_obj = dt_obj.replace(tzinfo=None)
        date_id = int(dt_obj.strftime('%Y%m%d'))

        insertion_date = datetime.now(pytz.utc).astimezone(eest).replace(tzinfo=None)

        return {
            'ID_Event': row['ID_Event'],
            'ID_Network_ID'      : net_map.get(row['LB_net']),
            'ID_RegionCountry_ID': reg_map.get((row['LB_Region'], row['LB_Country'])),
            'ID_type_ID'         : type_map.get(row['LB_type']),
            'ID_date_ID'         : date_id,
            'ID_depthCategory_ID': depth_map.get(row['LB_depthCategory'] or 'Shallow'),
            'ID_magCategory_ID'  : mag_map.get(row['LB_magCategory'] or 'Not Felt'),
            'VL_n_mag'           : float(row['VL_n_mag'])  if row['VL_n_mag']  is not None else None,
            'VL_n_depth'         : float(row['VL_n_depth'])if row['VL_n_depth']is not None else None,
            'LB_place'           : row['LB_place'],
            'DT_time'            : dt_obj,
            'VL_latitude'        : float(row['VL_latitude'])  if row['VL_latitude']  is not None else None,
            'VL_longitude'       : float(row['VL_longitude']) if row['VL_longitude'] is not None else None,
            'ID_nst'             : int(row['ID_nst']) if row['ID_nst'] is not None else None,
            'ID_gap'             : int(row['ID_gap']) if row['ID_gap'] is not None else None,
            'VL_dmin'            : float(row['VL_dmin']) if row['VL_dmin'] is not None else None,
            'VL_horizontalError' : float(row['VL_horizontalError']) if row['VL_horizontalError'] is not None else None,
            'ID_magNst'          : int(row['ID_magNst']) if row['ID_magNst'] is not None else None,
            '_DT_insertion_date': row['_DT_insertion_date'] if is_updated else insertion_date,
            '_DT_updated_date': updated_time if is_updated else None ,
            '_LB_data_source'    : row.get('_LB_data_source', 'T_ODS_day_earthquake')
        }

    fact_records = (
        ods_rows
        | 'EnrichFact' >> beam.Map(
            enrich_fact,
            net_map   = full_net_map,
            reg_map   = full_reg_map,
            type_map  = full_type_map,
            mag_map   = full_mag_map,
            depth_map = full_depth_map)
        | 'AttachJobID' >> beam.ParDo(_AttachJobId())
    )

    fact_records | 'WriteFactStaging' >> WriteToBigQuery(
        table=f'{PROJECT_ID}:{DW_DATASET}.T_FACT_Events_staging',
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        method='FILE_LOADS',
        schema=None,
        custom_gcs_temp_location=f'gs://{INTERMEDIATE_BUCKET}/temp',
     )

    #  RUN 
    result = p.run()
    result.wait_until_finish()
    if str(result.state) == 'DONE':
        _merge_fact_table()
        bq_client.delete_table(            
            f"{PROJECT_ID}.{DW_DATASET}.T_FACT_Events_staging",
            not_found_ok=True)
        
    print(f"Pipeline execution FINISHED at {datetime.now(EEST).strftime('%H:%M:%S')} EEST")

if __name__ == '__main__':
    run()
