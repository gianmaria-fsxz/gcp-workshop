PYTHON310 = "python:3.7"
TF_TRAINING_CONTAINER_IMAGE_URI = (
    "europe-docker.pkg.dev/vertex-ai/training/tf-cpu.2-6:latest"
)
TF_SERVING_CONTAINER_IMAGE_URI = (
    "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-6:latest"
)
SKL_TRAINING_CONTAINER_IMAGE_URI = (
    "europe-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest"
)
SKL_SERVING_CONTAINER_IMAGE_URI = (
    "europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest"
)

# ###########################################
# Required packages + versions for components
# ###########################################

# Ensure that these versions are in sync with Pipfile

# Google SDK specific
GOOGLE_CLOUD_BIGQUERY = "google-cloud-bigquery==2.30.0"
GOOGLE_CLOUD_STORAGE = "google-cloud-storage==1.42.2"
GOOGLE_CLOUD_AIPLATFORM = "google-cloud-aiplatform==1.10.0"
GOOGLE_CLOUD_COMPONENTS= 'google_cloud_pipeline_components==0.2.0'

# TF specific
TENSORFLOW = "tensorflow==2.7.1"
TENSORFLOW_DATA_VALIDATION = "tensorflow-data-validation==1.6.0"
TENSORFLOW_MODEL_ANALYSIS = "tensorflow-model-analysis==0.37.0"

# XGB specific
XGBOOST = "xgboost==1.4.2"
SKLEARN = "scikit-learn==0.24.1"

#PANDAS GBQ
PANDASGBQ = "pandas_gbq==0.19.1"
PANDAS = "pandas==1.3.2"
PYARROW = "pyarrow==9.0.0"

#GCSFS
GCSFS= "gcsfs==0.5.0"