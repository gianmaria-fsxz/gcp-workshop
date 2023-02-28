from kfp.v2.dsl import Output, Dataset, component
from pipelines.kfp_components.dependencies import PYTHON310, GOOGLE_CLOUD_BIGQUERY, GOOGLE_CLOUD_STORAGE, PANDAS,PYARROW

@component(base_image= PYTHON310, 
           packages_to_install=[GOOGLE_CLOUD_STORAGE, GOOGLE_CLOUD_BIGQUERY, PANDAS, PYARROW])
def read_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    dataset: Output[Dataset],
    
):
    from google.cloud import bigquery
    client = bigquery.Client(project = project_id)

    read_query = f"SELECT * FROM {project_id}.{dataset_id}.{table_id}"
    df = client.query(read_query).to_dataframe()

    df.to_csv(dataset.path + ".csv" , index=False, encoding='utf-8')

