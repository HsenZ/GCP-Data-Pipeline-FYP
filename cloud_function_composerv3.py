import functions_framework
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from google.auth.transport.requests import Request
from google.auth import default
import uuid
import time

# HTTP entrypoint
@functions_framework.http
def main(request):
    # Setup timezone (UTC+3)
    eest = timezone(timedelta(hours=3))
    now = datetime.now(eest)

    bucket_name = "imp-bucket"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    if now.day == 1:
        # 🟢 FULL LOAD
        first_day_this_month = now.replace(day=1)
        last_month = first_day_this_month - timedelta(days=1)
        start_date = last_month.replace(day=1).strftime("%Y-%m-%d")
        url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}"
        filename = f"whole_month_{last_month.strftime('%m')}.csv"
    else:
        # 🔁 DELTA LOAD
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.csv"
        timestamp = now.strftime("%Y%m%d-%H%M%S")
        filename = f"all_day_{timestamp}.csv"

    object_name = f"RAWDATA/{filename}"

    # Download the file
    resp = requests.get(url)
    if resp.status_code != 200:
        return f"Download failed: {resp.status_code}", 500

    # Upload to GCS
    bucket.blob(object_name).upload_from_string(resp.text, content_type="text/csv")
    print(f"Uploaded {filename} to GCS")

    # Optional wait
    print("Waiting 2 minutes before triggering DAG...")
    time.sleep(120)

    # Airflow DAG trigger info
    dag_id = "load_pipeline_controller"
    dag_run_id = f"triggered__{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"

    credentials, _ = default()
    credentials.refresh(Request())

    dag_trigger_url = (
        "https://composer.googleapis.com/v1/projects/imp-fyp/locations/us-central1/environments/load-orchestrator-v3/dags/load_pipeline_controller:trigger
"
        
    )

    headers = {
    "Authorization": f"Bearer {credentials.token}",
    "Content-Type": "application/json"
    }

    payload = {
    "executionId": dag_run_id,   # similar to dag_run_id in Composer 2
    "conf": {
        "filename": filename
    }
    }


    try:
        dag_response = requests.post(dag_trigger_url, headers=headers, json=payload)
        if dag_response.status_code in [200, 201]:
            print(f"DAG '{dag_id}' triggered successfully with run_id '{dag_run_id}'")
        else:
            print(f"DAG trigger failed: {dag_response.status_code}")
            print(dag_response.text)
            return f"DAG trigger failed: {dag_response.status_code}", 500
    except Exception as e:
        print(f"Exception while triggering DAG: {e}")
        return f"Exception during DAG trigger: {e}", 500

    return f"{filename} uploaded and DAG '{dag_id}' triggered successfully.", 200
