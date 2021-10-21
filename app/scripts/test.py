import modin.pandas as pd

class RawTest:
    def __init__(self,sample:dict):
        self.sample=sample

    def column_test(self): #df detect
        commoncol=set.intersection(*[set(list(self.sample[i].columns)) for i in self.sample.keys()])
        cat=0
        file_col=[]
        for i in self.sample:
            extra=set.union(*[set(list(self.sample[i].columns)),commoncol])-set.intersection(*[set(list(self.sample[i].columns)),commoncol])
            file_col.append([i,list(commoncol),list(self.sample[i].columns),list(extra)])
        test_result=pd.DataFrame(
                                columns=['Filename','Common Col','Files Columns','Extra Columns'],
                                data=file_col
                                )
        return test_result

    def missing_records_test(self):
        records=0
        for i in self.sample:
            records=records+self.sample[i].shape[0]
        return records
    
class StagingTest:
    def __init__(self,sample):
        self.sample=sample
    
    def missing_records_test(self):
        records=0
        for i in self.sample:
            records=records+self.sample[i].shape[0]
        return records

    def overview_test(self,config):
        date_values,date_percentage,num_values,num_percentage,index=[],[],[],[],[]
        for mydict in config:
            for col,coltype in mydict.items():
                if coltype == 'datetime64[ns]':
                    wrong_value=self.sample.loc[pd.to_datetime(self.sample[col], errors='coerce').isna()==True,col].count()
                else:
                    wrong_value=0
                date_values.append(int(wrong_value))
                date_percentage.append(int(wrong_value)/len(self.sample))
                if coltype=='float64' or coltype=='int64':
                    wrong_value=self.sample.loc[pd.to_numeric(self.sample[col], errors='coerce').isna()==True,col].count()
                else:
                    wrong_value=0
                num_values.append(int(wrong_value))
                num_percentage.append(int(wrong_value)/len(self.sample))
                index.append(col)

        exam=pd.concat((self.sample.isnull().sum().rename('Total Missing Value'),
                        self.sample.isnull().mean().apply(lambda x : '{:.2f}%'.format(x*100)).rename('% Missing Value'),
                        pd.Series(data=date_values,index=index).rename('Wrong Date Value'),
                        pd.Series(data=date_percentage,index=index).apply(lambda x : '{:.2f}%'.format(x*100)).rename('% Wrong Date Value'),
                        pd.Series(data=num_values,index=index).rename('Wrong Numeric Value'),
                        pd.Series(data=num_percentage,index=index).apply(lambda x : '{:.2f}%'.format(x*100)).rename('% Wrong Numeric Value')
                        ),axis=1)
        return exam
    
    def nan_test(self,col):
        test_df=self.sample.loc[self.sample[col].isna()==True]
        return test_df
    
    def inscope_test(self,col):
        test_df=self.sample[col].value_counts().to_frame()
        test_df['inscope']='yes'
        return test_df
    
    def date_test(self,col):
        test_df=self.sample.loc[pd.to_datetime(self.sample[col], errors='coerce').isna()==True]
        return test_df
    
    def numeric_test(self,col):
        test_df=self.sample.loc[pd.to_numeric(self.sample[col], errors='coerce').isna()==True]
        return test_df

    def positive_test(self,col):
        test_df=self.sample.loc[self.sample[col]<0]
        return test_df


        
