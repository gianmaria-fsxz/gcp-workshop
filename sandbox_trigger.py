import argparse
import base64
import json
import logging
import os
import distutils.util
from typing import Optional, List

from google.cloud import aiplatform


def trigger_pipeline(
    project_id: str,
    location: str,
    template_path: str,
    pipeline_root: str,
    service_account: str,
    enable_caching: Optional[bool] = True,
):
    """Trigger the Vertex Pipeline run.
    Args:
        project_id (str): GCP Project ID in which to run the Vertex Pipeline
        location (str): GCP region in which to run the Vertex Pipeline
        template_path (str): local or GCS path containing the (JSON) KFP
        pipeline definition
        pipeline_root (str): GCS path to use as the pipeline root (for passing
         metadata/artifacts within the pipeline)
        parameter_values (dict): dictionary containing the input parameters
        for the KFP pipeline
        service_account (str): email address of the service account that
        should be used to execute the ML pipeline in Vertex
        encryption_spec_key_name (Optional[str]): Cloud KMS resource ID
        of the customer managed encryption key (CMEK) that will protect the job
        network (Optional[str]): name of Compute Engine network to
        which the job should be visible
        enable_caching (Optional[bool]): Whether to enable caching of pipeline
        component results if component+inputs are the same. Defaults to None
        (enable caching, except where disabled at a component level)
    """

    # Initialise API client
    aiplatform.init(project=project_id, location=location)
    print("aiplatform initialized")
    # Instantiate PipelineJob object
    pl = aiplatform.pipeline_jobs.PipelineJob(
        # Display name is required but seemingly not used
        # see
        # https://github.com/googleapis/python-aiplatform/blob/9dcf6fb0bc8144d819938a97edf4339fe6f2e1e6/google/cloud/aiplatform/pipeline_jobs.py#L260 # noqa
        display_name=template_path,
        enable_caching=enable_caching,
        template_path=template_path,
        pipeline_root=pipeline_root,
    )

    # Execute pipeline in Vertex
    pl.submit(
        service_account=service_account,
    )

    return pl


def get_env() -> dict:
    """Get the necessary environment variables for pipeline runs,
    and return them as a dictionary.
    """

    project_id = os.environ["VERTEX_PROJECT_ID"]
    location = os.environ["VERTEX_LOCATION"]
    pipeline_root = os.environ["VERTEX_PIPELINE_ROOT"]
    service_account = os.environ["VERTEX_SA_EMAIL"]
    template_path = os.environ["PIPELINE_FILE"]
    # For CMEK and network, we want an empty string to become None, so we add "or None"

    return {
        "project_id": project_id,
        "location": location,
        "pipeline_root": pipeline_root,
        "service_account": service_account,
        "template_path": template_path
    }



if __name__ == "__main__":
    env = get_env()
    trigger_pipeline(
        project_id = env["project_id"],
        location = env["location"],
        template_path = env["template_path"],
        pipeline_root = env["pipeline_root"],
        service_account = env["service_account"]
        )

# aiplatform.init(project='sandbox-mvp-001',
#                 location='europe-west1')

# job = aiplatform.PipelineJob(
#     display_name="basic-pipeline-imputer",
#     template_path="training.json"
# )

# job.run()