from kfp.v2.dsl import Output, Dataset, component
from pipelines.kfp_components.dependencies import PYTHON310, GOOGLE_CLOUD_BIGQUERY, GOOGLE_CLOUD_STORAGE, PANDAS,PYARROW
from pipelines.kfp_components.variables import TARGET_COL, KEY_COL, BINARY_COLS, INT_COLS, FLOAT_COLS

@component(base_image= PYTHON310, 
           packages_to_install=[GOOGLE_CLOUD_STORAGE, GOOGLE_CLOUD_BIGQUERY, PANDAS, PYARROW])
def transfer_to_bq(
    uri: str,
    project: str,
    dataset: str,
    table: str    
):

    from google.cloud import bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client(project= project)
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(autodetect=True,
                                        skip_leading_rows=1,
                                        source_format=bigquery.SourceFormat.CSV,
                                    )

    file_name = "train_auto.csv"
    # uri = f"gs://{bucket_name}/{file_name}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

    destination_table