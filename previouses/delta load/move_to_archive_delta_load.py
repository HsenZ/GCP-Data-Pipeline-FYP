import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.filesystems import FileSystems
import os
import re

# ---------------- CONFIGURE ----------------
PROJECT_ID = 'imp-fyp'
BUCKET = 'imp-bucket'
REGION = 'us-central1'
SOURCE_PATTERN = f'gs://{BUCKET}/RAWDATA/all_day_*.csv'
ARCHIVE_FOLDER_ROOT = f'gs://{BUCKET}/ARCHIVE'
STAGING_BUCKET = 'dataflow-intermediate-bucket'
# -------------------------------------------

def move_and_archive_file(metadata):
    source_path = metadata.path
    filename = os.path.basename(source_path)

    # Extract date from filename: all_day_YYYYMMDD_HHMMSS.csv
    match = re.search(r'all_day_(\d{8})[-_]\d{6}', filename)
    if not match:
        raise ValueError(f"Filename {filename} does not match expected pattern.")

    date_part = match.group(1)
    archive_filename = f'archive_{filename}'
    archive_path = f'{ARCHIVE_FOLDER_ROOT}/DELTA_LOAD/{date_part}/{archive_filename}'

    print(f"Moving {source_path} → {archive_path}")
    FileSystems.copy([source_path], [archive_path])
    FileSystems.delete([source_path])
    print(f"Moved and deleted original: {source_path}")
    return archive_path

def run():
    options = PipelineOptions()
    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    gco.job_name = 'move-all-to-archive'
    gco.staging_location = f'gs://{STAGING_BUCKET}/staging'
    gco.temp_location = f'gs://{STAGING_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Match RAW files' >> beam.Create([SOURCE_PATTERN])
            | 'Get matched metadata' >> beam.FlatMap(lambda pattern: FileSystems.match([pattern])[0].metadata_list)
            | 'Move each file' >> beam.Map(move_and_archive_file)
        )

if __name__ == '__main__':
    run()
