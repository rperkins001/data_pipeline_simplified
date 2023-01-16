from google.cloud import dataproc_v1

class DataProc:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = dataproc_v1.ClusterControllerClient()

    def create_cluster(self, cluster_name, zone):
        """
        Creates a Dataproc cluster with the specified name and zone.
        """
        # Set cluster config
        config = {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-1"
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-1"
            }
        }

        # Create the cluster
        cluster = {
            "project_id": self.project_id,
            "cluster_name": cluster_name,
            "config": config,
            "region": zone
        }
        operation = self.client.create_cluster(project_id=self.project_id, region=zone, cluster=cluster)
        operation.result()
        print(f'Cluster {cluster_name} created.')

    def submit_job(self, cluster_name, job_name, file_content):
        """
        Submits a job to the specified Dataproc cluster.
        """
        # Set job config
        job_config = {
            "placement": {
                "cluster_name": cluster_name
            },
            "pyspark_job": {
                "main_python_file_uri": file_content
            }
        }

        # Submit the job
        job = {
            "project_id": self.project_id,
            "job_name": job_name,
            "config": job_config
        }
        operation = self.client.submit_job(project_id=self.project_id, region='global', job=job)
        operation.result()
        print(f'Job {job_name} submitted.')
        
    # Additional functions:
    
    def list_clusters(self):
        """
        Lists all the clusters in the project.
        """
        response = self.client.list_clusters(project_id=self.project_id, region='global')
        clusters = response.clusters
        print("Clusters:")
        for cluster in clusters:
            print(cluster.cluster_name)

    def delete_cluster(self, cluster_name):
        """
        Deletes a specific cluster by name.
        """
        self.client.delete_cluster(project_id=self.project_id, region='global', cluster_name=cluster_name)
        print(f'Cluster {cluster_name} deleted.')

    def get_job(self, job_id):
        """
        Retrieves the details of a specific job by its job ID.
        """
        job = self.client.get_job(project_id=self.project_id, region='global', job_id=job_id)
        print(job)
        
    def list_jobs(self, state):
        """
        Lists all the jobs in a project filtered by state.
        """
        response = self.client.list_jobs(project_id=self.project_id, region='global', filter=f'state = {state}')
        jobs = response.jobs
        print(f"Jobs in state {state}:")
        for job in jobs:
            print(job.job_id)

    def cancel_job(self, job_id):
        """
        Cancels a running or queued job by its job ID.
        """
        self.client.cancel_job(project_id=self.project_id, region='global', job_id=job_id)
        print(f'Job {job_id} canceled.')
              
    def get_cluster(self, cluster_name):
        """
        Retrieves the details of a specific cluster by its name.
        """
        cluster = self.client.get_cluster(project_id=self.project_id, region='global', cluster_name=cluster_name)
        print(cluster)
        
    def set_cluster_config(self, cluster_name, config):
        """
        Set the config for a specific cluster
        """
        self.client.update_cluster(project_id=self.project_id, region='global', cluster_name=cluster_name, update_mask='config', cluster=config)
        
    def list_operation(self, filter):
        """
        Lists all the operations in the project filtered by the filter
        """
        response = self.client.list_operations(project_id=self.project_id, region='global', filter=filter)
        operations = response.operationsß
        for operation in operations:
            print(operation.name)
            
    def get_operation(self, operation_name):
        """
        Retrieves the details of a specific operation by its name.
        """
        operation = self.client.get_operation(project_id=self.project_id, region='global', operation_name=operation_name)
        print(operation)
    