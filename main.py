import json
from google.cloud import storage, logging
from dataproc import DataProc
from bigquery import Bigquery

def process_data(event, context, cluster_name, job_name, dataset_id, table_id):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
         cluster_name (str): The name of the cluster to submit the job to.
         job_name (str): The name of the job to submit.
         dataset_id (str): The ID of the dataset to load data into in BigQuery.
         table_id (str): The ID of the table to load data into in BigQuery.
    """
    try:
        # Create a logger client
        logger = logging.Client().logger("process_data")
        
        file = event
        bucket_name = file['bucket']
        file_name = file['name']
        project_id = event['projectId']
        
        # Create a Storage client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Get the file
        blob = bucket.get_blob(file_name)

        # Download the file
        file_content = blob.download_as_string()

        # Validate the file format
        try:
            data = json.loads(file_content)
        except json.decoder.JSONDecodeError as e:
            raise ValueError("Invalid file format") from e
        
        # Create a DataProc client
        dataproc_client = DataProc(project_id)
    
        # Submit a job to the cluster
        dataproc_client.submit_job(cluster_name, job_name, file_content)

        # Create a Bigquery client
        bigquery_client = Bigquery(project_id)

        # Load data into Bigquery
        bigquery_client.load_data(dataset_id, table_id, file_content)
        
        logger.info("Data processing completed successfully")
    except Exception as e:
        logger.error("Error occurred while processing data: %s", e)
        raise
