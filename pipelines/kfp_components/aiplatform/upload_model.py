from kfp.v2.dsl import component,Output,Input,Model, Artifact

@component(base_image= "gcr.io/deeplearning-platform-release/sklearn-cpu.0-23", 
           packages_to_install=["gcsfs","google-cloud-aiplatform"],
           )
def upload_model(
    display_name: str,
    serving_container_image_uri: str,
    model: Input[Model],
    project_location: str,
    project_id: str,
) -> str:
    """
    Fetch a model given a model name (display name) and export to GCS.

    Args:
        display_name (str): Required. The display name of the Model. The name can
        be up to 128 characters long and can be consist of any UTF-8 characters.
        serving_container_image_uri (str): Required. The URI of the Model serving
            container.
        model (Input[Model]): Model to be uploaded.
        project_location (str): location of the Google Cloud project
        project_id (str): project id of the Google Cloud project
        description (str): The description of the model. Defaults to None.
        sync (bool): Upload model synchronously. Defaults to True.

    Returns:
        str: Resource name of the exported model.
    """

    import logging
    from google.cloud.aiplatform import Model as aip_model

    # uri expects a folder containing the model binaries
    # artifact_uri = model.uri.rsplit("/", 1)[0] + "/model"
    artifact_uri = model.uri.replace("model", "")

    logging.info("upload model...")
    model = aip_model.upload(
        display_name=display_name,
        serving_container_image_uri=serving_container_image_uri,
        artifact_uri=artifact_uri,
        project=project_id,
        location=project_location
    )

    logging.info(f"uploaded model {model}")

    return model.resource_name