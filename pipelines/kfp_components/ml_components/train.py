from kfp.v2.dsl import Artifact, Input, Output, Dataset, component
from pipelines.kfp_components.dependencies import XGBOOST, SKLEARN, PANDAS, SKL_TRAINING_CONTAINER_IMAGE_URI


@component(base_image= SKL_TRAINING_CONTAINER_IMAGE_URI, 
           packages_to_install=[PANDAS, SKLEARN] 
)
def train(
    dataset:  Input[Dataset],
    target_col: str, 
    key_col: str,
    model:    Output[Artifact], 
):
    
    from sklearn.tree import DecisionTreeClassifier
    import pandas
    import pickle

    data = pandas.read_csv(dataset.path+".csv")
    x = data[list(set(data.columns) - set([target_col])- set([key_col]))]
    y = data[target_col]
    
    file_name = model.path + f".pkl"    
    model_dtr = DecisionTreeClassifier(random_state=0).fit(x, y)
    
    with open(file_name, 'wb') as file:  
        pickle.dump(model_dtr, file)