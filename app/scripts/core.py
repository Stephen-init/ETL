import modin.pandas as pd
import os

class TargetFiles:
    def __init__(self,directory):
        self.directory=directory

    def folder_reader(self,filter=None):
        if filter is not None:
            files_list = [os.path.join(root, name)
                        for root, dirs, files in os.walk(self.directory)
                        for name in files
                        if name.endswith((".xlsx", ".xls",'csv')) 
                            and all( x in name.lower() for x in  filter['include'] if x is not None) 
                            and all( x not in name.lower() for x in filter['exclude'] if x is not None) 
                            and all( x not in name.lower() for x in filter['broken'] if x is not None)                
                            ]
        else:
            files_list = [os.path.join(root, name)
                        for root, dirs, files in os.walk(self.directory)
                        for name in files
                        if name.endswith((".xlsx", ".xls",'csv'))]            

        return files_list
    
    def read_files(self,readrows=None):
        inputs_dict={}
        for filename in self.folder_reader():
            if filename.endswith(".xlsx"):
                myfile=pd.read_excel(filename,nrows=readrows)
                inputs_dict[filename.split("/")[-1]]=myfile
            else:
                myfile=pd.read_csv(filename,nrows=readrows)
                inputs_dict[filename.split("/")[-1]]=myfile
        return inputs_dict

class ReadFiles:
    def __init__(self,directory):
        self.directory=directory

    def read_files(self):
        if self.directory.endswith((".xlsx",".xls")):
            myfile=pd.read_excel(self.directory)
            myfile['source']=self.directory.split("/")[-1]
        else:
            myfile=pd.read_csv(self.directory)
            myfile['source']=self.directory.split("/")[-1]
        return myfile
    
class cleaning:
    def __init__(self,df:pd.DataFrame):
        self.df=df
    
    def duplicates(self):
        dups=self.df.loc[self.df.duplicated(keep=False)==True]
        self.df=pd.concat([self.df, dups]).drop_duplicates(keep=False)
        result=[self.df,dups]
        return result

    def nan_values(self):
        nan_df=self.df.loc[self.df.isna().any(axis=1)==True]
        self.df=pd.concat([self.df, nan_df]).drop_duplicates(keep=False)
        result=[self.df,nan_df]
        return result
    
    def outliers(self):
        non_ascii=pd.DataFrame()
        for i in self.df.columns.to_list():
            df = self.df.loc[self.df[i].astype(str).map(str.isascii)==False]
            non_ascii=pd.concat([non_ascii,df])
        non_ascii=non_ascii.drop_duplicates(keep=False)
        self.df=pd.concat([self.df, non_ascii]).drop_duplicates(keep=False)
        result=[self.df,non_ascii]
        return result


    


    




