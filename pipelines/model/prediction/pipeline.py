from kfp.v2 import compiler, dsl

from pipelines.kfp_components.bigquery import read_table, transfer_to_bq
from pipelines.kfp_components.aiplatform import lookup_model
from pipelines.kfp_components.ml_components import preprocess, predict
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
              pipeline_root='gs://gcp-workshop-vertex-pipeline')
def workshop_prediction(
    input_uri: str = os.environ.get("INPUT_DATA"),
    dataset_id: str = os.environ.get("INPUT_DATASET"),
    table_id: str = os.environ.get("INPUT_TABLE"),
    project_id: str = os.environ.get("VERTEX_PROJECT_ID"),
    project_location: str = os.environ.get("VERTEX_LOCATION"),
    model_display_name: str = os.environ.get("MODEL_NAME_DISPLAY")
    ):

    import logging

    transfer = transfer_to_bq(uri=input_uri, 
                            project=project_id, 
                            dataset=dataset_id, 
                            table=table_id)
    
    data_op = read_table(project_id, 
                         dataset_id, 
                         table_id).after(transfer)

    preprocess_op = preprocess(dataset=data_op.outputs["dataset"],
                                )

    # #predict
    # lookup_op = lookup_model(
    #     model_display_name, project_location, project_id, fail_on_model_not_found=True)

    # predictions = predict(test_set=preprocess_op.outputs["dataset_test"],
    #                       model_dt = lookup_op.outputs["uri"],
    #                        )

                                   

if __name__ == "__main__":
    compile()
