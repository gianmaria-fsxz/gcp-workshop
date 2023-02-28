from kfp.v2.dsl import Input, Output, Dataset, Model, component, Artifact
from pipelines.kfp_components.dependencies import  PYTHON310, PANDAS, SKLEARN, GCSFS


@component(base_image= PYTHON310, 
           packages_to_install=[PANDAS,SKLEARN, GCSFS]
)
def predict(
    test_set:  Input[Dataset],
    model_dt: str,
    predictions: Output[Dataset],
):

    from sklearn.tree import DecisionTreeClassifier

    import pandas
    import pickle
    import os
    import gcsfs
    import logging

    fs = gcsfs.GCSFileSystem(project = 'sandbox-mvp-001')
    
    data = pandas.read_csv(test_set.path+".csv")
 
    model = DecisionTreeClassifier()
    # file_name = model_dtr + "model.pkl"
    modelpath = os.path.join(model_dt , 'model.pkl')
    logging.info("modelpath: {}".format(modelpath))
    
    with fs.open(modelpath, 'rb') as file:
        model = pickle.load(file)  

    y_pred = model.predict(data.values)
    data['prediction'] = y_pred
    data.to_csv(predictions.path   + ".csv" , index=False, encoding='utf-8')

   