from kfp.v2 import compiler, dsl

from pipelines.kfp_components.bigquery import read_table, transfer_to_bq
from pipelines.kfp_components.aiplatform import endpoint_predict
from pipelines.kfp_components.ml_components import preprocess
from pipelines.kfp_components.dependencies import SKL_SERVING_CONTAINER_IMAGE_URI

import os


def compile(template_path: str = os.environ.get("PIPELINE_FILE")):
    """
    Uses the kfp compiler package to compile the pipeline function into a workflow yaml

    Args:
        None

    Returns:
        None
    """
    compiler.Compiler().compile(
        pipeline_func=workshop_prediction,
        package_path=template_path,
        type_check=False,
    )


@dsl.pipeline(name="pipeline-predict-workshop",
             pipeline_root=os.environ.get("VERTEX_PIPELINE_ROOT"))

def workshop_prediction(
    input_uri: str = os.environ.get("INPUT_DATA"),
    dataset_id: str = os.environ.get("INPUT_DATASET"),
    input_table_id: str = os.environ.get("INPUT_TABLE"),
    project_id: str = os.environ.get("VERTEX_PROJECT_ID"),
    project_location: str = os.environ.get("VERTEX_LOCATION"),
    output_table_id: str= os.environ.get("OUTPUT_TABLE_ID"),
    endpoint_id: str = os.environ.get("ENDPOINT_ID")
    ):

    transfer = transfer_to_bq(uri=input_uri, 
                            project=project_id, 
                            dataset=dataset_id, 
                            table=input_table_id)
    
    data_op = read_table(project_id, 
                         dataset_id, 
                         input_table_id).after(transfer)

    preprocess_op = preprocess(dataset_raw=data_op.outputs["dataset"])

    predictions = endpoint_predict(endpoint_id=endpoint_id, 
                                   input_data=preprocess_op.outputs["dataset_processed"],
                                   project_id=project_id,
                                   project_location=project_location,
                                   dataset_id= dataset_id,
                                   table_id=output_table_id
                           ).set_display_name("endpoint_predictions")
                                   

if __name__ == "__main__":
    compile()
