==================================
Devicecycle - Modeling
==================================

launch step

- activate the virtual environment \ 
source /home/jupyter/vertex_venv/bin/activate

- loading the envoronment variables \
source env.sh

- compile the pipeline \
rm $PIPELINE_FILE
python -m pipelines.models.price_imputer.training.pipeline

- trigger the pipeline\
python sandbox_trigger.py
