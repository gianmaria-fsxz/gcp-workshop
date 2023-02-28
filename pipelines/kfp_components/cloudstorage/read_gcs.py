from kfp.v2.dsl import Output, Dataset, component


@component(base_image= "gcr.io/deeplearning-platform-release/sklearn-cpu.0-23", 
           packages_to_install=["gcsfs", "pickle5"])
def read_gcs(
    uri: str,
    dataset: Output[Dataset],
):
    import pandas as pd
    import numpy as np
    import pickle5 as pickle
    from sklearn.model_selection import train_test_split
    from gcsfs.core import GCSFileSystem

    '''
    import pickle5
    fs = gcsfs.GCSFileSystem(project = 'sandbox-mvp-001')
    fs.ls('vertex-bucket-devicecycle')

    file = fs.open("gs://vertex-bucket-devicecycle/input_imputer.pkl",'rb')
    datset = pickle.load(file)

    fs = gcsfs.GCSFileSystem(project = 'my-google-project')
    fs.ls('my-bucket')
    with fs.open('my-bucket/my-file.txt', 'rb') as file:
    print(pickle.load(file))
    '''

    ## mode to load project name from costants 
    fs = GCSFileSystem(project = 'sandbox-mvp-001')

    file = fs.open(uri,'rb')

    datset = pickle.load(file)

    with open(dataset.path + f".pkl" , 'wb') as file:  
        pickle.dump(datset, file)

    '''
    df = df.loc[df.max_price>5].reset_index()
      
    key_col = 'phone'
    target_col = 'max_price'

    # X = pd.get_dummies(df[list(set(df.columns) - set([key_col]) - set([target_col]))])
    X = pd.get_dummies(df[list(set(df.columns) - set([key_col]))])

    train,test  = train_test_split(X, test_size=.2, random_state=2)
    # y_train, y_test = train_test_split(y, test_size=.2, random_state=2)
    
    train.to_csv(dataset_train.path + ".csv" , index=False, encoding='utf-8')
    test.to_csv(dataset_test.path + ".csv" , index=False, encoding='utf-8')
    '''