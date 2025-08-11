import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.filesystems import FileSystems
import os
import re

# ---------------- CONFIGURE ----------------
PROJECT_ID = 'imp-fyp'
BUCKET = 'imp-bucket'
REGION = 'us-central1'
SOURCE_PATTERN = f'gs://{BUCKET}/RAWDATA/whole_month_*.csv'
ARCHIVE_FOLDER_ROOT = f'gs://{BUCKET}/ARCHIVE'
STAGING_BUCKET = 'dataflow-intermediate-bucket'
# -------------------------------------------

def move_and_archive_file(metadata):
    source_path = metadata.path
    filename = os.path.basename(source_path)

    # Extract month from filename: whole_month_MM.csv or whole_month_YYYYMM.csv
    match = re.search(r'whole_month_(\d{2}|\d{6})', filename)
    if not match:
        raise ValueError(f"Filename {filename} does not match expected full-load pattern.")

    date_part = match.group(1)
    archive_filename = f'archive_{filename}'
    archive_path = f'{ARCHIVE_FOLDER_ROOT}/FULL_LOAD/{date_part}/{archive_filename}'

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
    gco.job_name = 'move-to-archive-full-load'
    gco.staging_location = f'gs://{STAGING_BUCKET}/staging'
    gco.temp_location = f'gs://{STAGING_BUCKET}/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Match full-load RAW files' >> beam.Create([SOURCE_PATTERN])
            | 'Get matched metadata' >> beam.FlatMap(lambda pattern: FileSystems.match([pattern])[0].metadata_list)
            | 'Move full-load file' >> beam.Map(move_and_archive_file)
        )

if __name__ == '__main__':
    run()
