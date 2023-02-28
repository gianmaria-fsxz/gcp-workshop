#!/bin/bash
SCOPE=$1

echo Pipeline con scope $2
echo Venv activation...
source /home/11622867/wrk_vertex_venv/bin/activate
echo venv attivato
echo esportazione variabili...

source ./env.sh $SCOPE

echo $PIPELINE_FILE
python -m pipelines.model.$SCOPE.pipeline
python sandbox_trigger.py