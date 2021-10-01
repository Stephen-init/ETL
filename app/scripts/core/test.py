import pandas as pd
import datetime,os
import numpy as np
import copy

class RawTest:
    def __init__(self,sample:dict):
        self.sample=sample
    
    def sheet_test(self): #milti-sheets detect
        result=[]
        for i in self.sample:
            if isinstance(self.sample[i], dict):
                allcol=set.union(*[set(list(self.sample[i][j].columns)) for j in self.sample[i].keys()])
                comcol=set.intersection(*[set(list(self.sample[i][j].columns)) for j in self.sample[i].keys()])
                if allcol!=comcol:
                    test=self.column_test(self.sample[i])
                    test['File']=i
                    result.append(test)
        result = pd.concat(result)
        return result

    def column_test(self,sample): #df detect
        sample_dict=copy.deepcopy(sample)
        dict_instance=[]
        for i in sample_dict:
            if isinstance(sample_dict[i], dict):
                dict_instance.append(i)
        for i in dict_instance:
            sample_dict.pop(i,None)      
        file_col=[]
        commoncol=set.intersection(*[set(list(sample_dict[i].columns)) for i in sample_dict.keys()])
        cat=0
        for i in sample_dict:
            if isinstance(sample_dict[i], dict)==False:
                extra=set.union(*[set(list(sample_dict[i].columns)),commoncol])-set.intersection(*[set(list(sample_dict[i].columns)),commoncol])
                file_col.append([i,list(commoncol),list(sample_dict[i].columns),list(extra)])
        test_result=pd.DataFrame(
                                columns=['Filename','Common Col','Files Columns','Extra Columns'],
                                data=file_col
                                )
        return test_result

    def missing_values_table(self):
        sample_dict=copy.deepcopy(self.sample)
        dict_instance=[]
        for i in sample_dict:
            if isinstance(sample_dict[i], dict):
                dict_instance.append(i)
        for i in dict_instance:
            sample_dict.pop(i,None) 
        
        for i in sample_dict:
            mis_val = sample_dict[i].isnull().sum()
            mis_val_percent = 100 * sample_dict[i].isnull().sum() / len(sample_dict[i])
            mis_val_table = pd.concat([mis_val, mis_val_percent],
                                    axis=1)
            mis_val_table_ren_columns = mis_val_table.rename(
                columns={0: 'Missing Values', 1: '% of Total Values'})

            mis_val_table_ren_columns = (mis_val_table_ren_columns[
                mis_val_table_ren_columns.iloc[:, 1] != 0].sort_values(
                '% of Total Values', ascending=False).round(1))
            mis_val_table_ren_columns['file']=i.split("/")[-1]

        return mis_val_table_ren_columns

    def CleanData(df, drop_columns, target_name):
        interim_df = df.drop(columns=drop_columns)
        interim_df_2 = (interim_df
                        .drop_duplicates(ignore_index=True))
        cleaned_df = (interim_df_2
                        .dropna(subset=[target_name], how="any")
                        .reset_index(drop=True))
        return cleaned_df


                   
        
