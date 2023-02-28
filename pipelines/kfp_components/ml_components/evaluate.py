from kfp.v2.dsl import Artifact, Input, Output, Dataset, Model, component, Artifact
from pipelines.kfp_components.dependencies import  PYTHON310, PANDAS, SKLEARN
from typing import NamedTuple


@component(base_image= PYTHON310, 
           packages_to_install=[PANDAS,SKLEARN]
)
def evaluate(
    test_set:  Input[Dataset],
    model_dtr: Input[Artifact],
    target_col: str,
    key_col: str,
    monitoring_class: str,
    monitoring_metric: str,
    threshold: float,
    metrics: Output[Artifact],
) -> NamedTuple("output", [("deploy", str)]):

    from sklearn.tree import DecisionTreeClassifier
    from sklearn.metrics import classification_report

    import pandas
    import pickle
    import json
    import logging

    data = pandas.read_csv(test_set.path+".csv")

    x_test = data[list(set(data.columns) - set([target_col])- set([key_col]))]
    y_test = data[target_col].astype("int")


    model = DecisionTreeClassifier()
    file_name = model_dtr.path + ".pkl"
    with open(file_name, 'rb') as file:  
        model = pickle.load(file)
    

    y_pred = model.predict(x_test)   
    
    metrics_dict = classification_report(y_test, y_pred, output_dict = True)
    score = float(metrics_dict[monitoring_class][monitoring_metric])

    with open(metrics.path + ".json", "w") as f:
        json.dump(metrics_dict, f)

    if score > threshold:
        deploy = 'true'
    else:
        deploy = 'false'
    
    logging.info(f"Deploy check\n{monitoring_metric} on class {monitoring_class}: {round(score, 2)}")
    
    return (deploy,)