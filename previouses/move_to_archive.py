import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.filesystems import FileSystems
import datetime

# ---------------- CONFIGURE THESE PARAMETERS ----------------
PROJECT_ID = 'imp-fyp'
BUCKET = 'imp-bucket'
SOURCE_FILE_PATH = f'gs://{BUCKET}/all_day.csv'
ARCHIVE_FOLDER_ROOT = f'gs://{BUCKET}/ARCHIVE'
REGION = 'us-central1'
# ------------------------------------------------------------

def move_and_rename_file(element):
    current_date = datetime.datetime.utcnow().strftime('%Y%m%d')
    destination_path = f"{ARCHIVE_FOLDER_ROOT}/{current_date}/archive_{current_date}_all_day.csv"

    print(f"Checking existence of source: {SOURCE_FILE_PATH}")
    if not list(FileSystems.match([SOURCE_FILE_PATH])):
        raise FileNotFoundError(f"Source file does not exist: {SOURCE_FILE_PATH}")

    print(f"Copying {SOURCE_FILE_PATH} to {destination_path}")
    FileSystems.copy([SOURCE_FILE_PATH], [destination_path])

    print(f"Deleting original {SOURCE_FILE_PATH}")
    FileSystems.delete([SOURCE_FILE_PATH])

    print(f"File moved and renamed successfully to {destination_path}")
    return destination_path

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.region = REGION
    google_cloud_options.job_name = 'move-and-rename-file-job'
    google_cloud_options.staging_location = f'gs://dataflow-intermediate-bucket/staging'
    google_cloud_options.temp_location = f'gs://dataflow-intermediate-bucket/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Create dummy input' >> beam.Create(['trigger'])
            | 'Move and Rename File' >> beam.Map(move_and_rename_file)
        )

if __name__ == '__main__':
    run()
