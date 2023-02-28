from kfp.v2.dsl import Input, Output, Dataset, component
from pipelines.kfp_components.dependencies import PYTHON310, PANDAS, SKLEARN, PYARROW
from typing import NamedTuple


@component(base_image= PYTHON310, 
           packages_to_install=[PANDAS,SKLEARN,PYARROW])
def preprocess(
    dataset_raw:  Input[Dataset],
    dataset_processed: Output[Dataset],
    ):
 
    import pandas as pd
    from sklearn.preprocessing import StandardScaler

    # This object performs several preprocessing of the data set

    class Preprocessor: 

        def __init__(self, df):
            Preprocessor.data = df.copy()
            Preprocessor.processed_data = None

        def dropVar(self, vars):
            # Remove a variable from the dataset
            self.processed_data = self.data.drop(vars, axis=1)

        def str2int(self, variables):
            # Define a function that extract the numerical value from a string like '$200' and convert it to a float
            newdf = self.processed_data.copy()
            for var in variables:
                if newdf[var].dtypes == 'object': 
                    # if it was a string, convert variable to float 
                    newdf[var] = newdf[var].apply(lambda x: float(x.split("$")[1].replace(",","")) if type(x) == str else x)
                else: 
                    pass
            self.processed_data = newdf


        def fillMissing(self):  
            # fill NaN with mean and mode
            df = self.processed_data.copy()
            vars = df.columns
            for var in vars: 
                if any(df[var].isna()):
                    if df[var].dtypes == 'float64':
                        # substitute NaN with mean
                        df[var].fillna(value=df[var].mean(), 
                                            inplace=True)
                    else:
                        # substitute NaN with mode
                        df[var].fillna(value=df[var].mode()[0], 
                                            inplace=True)
            self.processed_data = df
                        
        def scaling(self, catVars, target):
            # setup scaler
            scaler = StandardScaler()
            df = self.processed_data.copy()

            scaleCols = list(set(df.columns)-set(catVars+[target]))
            # remove both target variables because in the test data they are not provided
            # training set scaling
            df[scaleCols] = scaler.fit_transform(df[scaleCols])        # re-add categorical variables
            # df = df.join(df[catVars])
            self.processed_data = df

        def cat2int(self, variables):
            # Convert categorical variables to numerical
            # Create a copy of the original dataframe to avoid warnings
            df = self.processed_data.copy()
            firstidx = df.index[0]
            for var in variables:
                if type(df[var][firstidx]) == str:
                    df[var] = df[var].apply(lambda x: 1 if 'yes' in x.lower() else 0)
                    c = 1
                else:
                    # if we run the code again it will
                    pass
            self.processed_data = df
        
        
        def convertCategorical(self):
            catVar = ['PARENT1', 'MSTATUS', 'RED_CAR', 'REVOKED']  
            # Convert categorical to dummies
            self.cat2int(catVar)
            
            df = self.processed_data.copy()
            # Convert binary to dummies
            # converting SEX
            df['GENDER'] = df['SEX'].apply(lambda x: 1 if 'M' in x else 0)
            df.drop(['SEX'],axis=1,inplace=True)

            # converting CAR_USE, URBANICITY
            df['COMMERCIAL_CAR_USE'] = df['CAR_USE'].apply(lambda x: 1 if 'commercial' in x.lower() else 0)
            df.drop(['CAR_USE'],axis=1,inplace=True)

            df['URBAN_CAR'] = df['URBANICITY'].apply(lambda x: 1 if 'urban' in x.lower() else 0)
            df.drop(['URBANICITY'],axis=1,inplace=True)

            # Convert EDUCATION, JOB, CAR_TYPE
            df['EDUCATION'] = df['EDUCATION'].apply(lambda x: 'Elementary Education' if '<high school' in x.lower() else x)
            # Insert dummy variables and Drop the original variable
            df = df.join(pd.get_dummies(df.EDUCATION.str.upper())).drop(['EDUCATION'],axis=1)
            df = df.join(pd.get_dummies(df.JOB.str.upper())).drop(['JOB'],axis=1)
            df = df.join(pd.get_dummies(df.CAR_TYPE.str.upper())).drop(['CAR_TYPE'],axis=1)

            df.columns = df.columns.str.replace("Z_","")
            df.columns = df.columns.str.replace(" ","_")
            
            self.processed_data = df    
    

    def preprocessing(df,train=False):
        print('\n'+'*'*10 + 'Preprocessing' + '*'*10)
        # define a Preprocessor object
        preprocess = Preprocessor(df)

        # Remove useless columns from the dataframes
        toDrop = ['TARGET_AMT'] if train else ['TARGET_FLAG','TARGET_AMT']
        toDrop += ['INDEX']
        preprocess.dropVar(toDrop)
        cat2numVars = ["INCOME","HOME_VAL","BLUEBOOK","OLDCLAIM"]
        preprocess.str2int(cat2numVars)
        
        # return preprocess.processed_data

        catcols = ['PARENT1', 'MSTATUS', 'RED_CAR', 'REVOKED',
                    'SEX','CAR_USE','URBANICITY','JOB','CAR_TYPE','EDUCATION']

        preprocess.scaling(catcols,target='TARGET_FLAG')
        preprocess.convertCategorical()
        preprocess.fillMissing()


        return preprocess.processed_data
    
    df_test = pd.read_csv(dataset_raw.path+".csv")
    df_processed  = preprocessing(df_test,False)
    
    df_processed.to_csv(dataset_processed.path   + ".csv" , index=False, encoding='utf-8')