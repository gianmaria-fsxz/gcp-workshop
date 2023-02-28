#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCOPE=$1

if [[ $SCOPE == "training" ]]; then
    export PIPELINE_FILE=training.json
    export PAYLOAD=dev.json
    export PIPELINE_FILES_GCS_PATH=gs://vertex-bucket-devicecycle/pipelines
    export PIPELINE_TEMPLATE=catboost
    export VERTEX_LOCATION=europe-west1
    export VERTEX_PIPELINE_ROOT=gs://vertex-bucket-devicecycle/pipeline_root
    export VERTEX_PROJECT_ID=sandbox-mvp-001
    export VERTEX_SA_EMAIL=sandbox-mvp-001@appspot.gserviceaccount.com
    export VERTEX_CONTAINER_URI=europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest
    export INPUT_DATASET=scraped_devices_test_20221205145612
    export INPUT_TABLE=labelled_20220701_joined
    export MODEL_NAME_DISPLAY=cloud_chip
fi

if [[ $SCOPE == "prediction" ]]; then
    export PIPELINE_FILE=predict.json
    export PAYLOAD=dev.json
    export VERTEX_LOCATION=europe-west4
    export VERTEX_PIPELINE_ROOT=gs://gcp-workshop-vertex-pipeline/pipeline_root
    export VERTEX_PROJECT_ID=sandbox-mvp-001
    export VERTEX_SA_EMAIL=sandbox-mvp-001@appspot.gserviceaccount.com
    export VERTEX_CONTAINER_URI=europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest
    export INPUT_DATA=gs://gcp-workshop-input/test_auto.csv
    export INPUT_DATASET=gcp_workshop
    export INPUT_TABLE=test_raw
    export VERTEX_MODEL_ID=autoinsurance_randomforestoo2tk3mu
    export ENDPOINT_ID=353928394934583296
    export OUTPUT_TABLE_ID=test_predictions
fi
