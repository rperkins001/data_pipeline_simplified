from google.cloud import bigquery

class Bigquery:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    def load_data(self, dataset_id, table_id, file_content, autodetect=False, schema=None):
        """
        Load data into Bigquery
        """
        job_config = bigquery.LoadJobConfig()
        if autodetect:
            job_config.autodetect = True
        else:
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            if schema:
                job_config.schema = schema
        job = self.client.load_table_from_string(file_content, dataset_id, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete
        print(f'Data loaded to {dataset_id}.{table_id}')

    def create_dataset(self, dataset_id):
        """
        Create a new dataset
        """
        dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        dataset = self.client.create_dataset(dataset) 
        print(f"Dataset {dataset.dataset_id} created.")

    def create_table(self, dataset_id, table_id, schema):
        """
        Create a new table
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table)
        print(f"Table {table.table_id} created.")

    def update_schema(self, dataset_id, table_id, schema):
        """
        Update schema of a table
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        original_schema = table.schema
        new_schema = original_schema[:]
        for field in schema:
            new_schema.append(bigquery.SchemaField(field["name"], field["type"]))
        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])
        print(f"Schema of table {table_id} updated.")

    def delete_table(self, dataset_id, table_id):
        """
        Delete a table
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        self.client.delete_table(table_ref)
        print(f"Table {table_id} deleted.")
    
    def query(self, query_string):
        """
        Run a query
        """
        query_job = self.client.query(query_string)
        results = query_job.result()  # Waits for job to complete
        return results