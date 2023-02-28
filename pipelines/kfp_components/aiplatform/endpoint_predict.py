from kfp.v2.dsl import component,Dataset,Input
from pipelines.kfp_components.dependencies import GOOGLE_CLOUD_AIPLATFORM, GOOGLE_CLOUD_STORAGE, PYTHON310, PANDASGBQ, SKLEARN

@component(base_image= PYTHON310, 
           packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, GOOGLE_CLOUD_STORAGE, PANDASGBQ, SKLEARN],
           )
def endpoint_predict(
    endpoint_id: str,
    input_data: Input[Dataset],
    project_location: str,
    project_id: str,
    dataset_id: str,
    table_id: str
):
    """
    Fetch a model given a model name (display name) and export to GCS.

    Args:
        endpoint_id (str): Required. The endpoint id
        input_data (Input[Dataset]): Data to predict.
        project_location (str): location of the Google Cloud project
        project_id (str): project id of the Google Cloud project
        dataset_id (str): bq dataset to write predictions
        table_id (str): table to write predictions

    Returns:
        str: Resource name of the exported model.
    """

    import logging
    from google.cloud import aiplatform
    import pandas as pd
    import pandas_gbq

    logging.info(f"Retrieving model endpoint in project_id = {project_id} and project_location = {project_location}...")
    
    endpoint = aiplatform.Endpoint(
            endpoint_name=endpoint_id,
            project=project_id,
            location=project_location)
    
    # logging.info("retrieved following endpoints...", endpoint.gca_resource)

    df_test = pd.read_csv(input_data.path+".csv")

    # logging.info(f"Test dataset read.")

    instances = df_test.to_dict(orient='records')
    predictions = endpoint.predict(instances)
    df_test['prediction'] = pd.DataFrame(predictions.predictions)['predicted_TARGET_FLAG']

    pandas_gbq.to_gbq(df_test, f"{dataset_id}.{table_id}", project_id=project_id)





