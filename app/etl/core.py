import pandas as pd
import datetime,os

class Raw:
    def __init__(self,directory,filter):
        self.directory=directory
        self.filter=filter

    def folder_reader(self):
        files_list = [os.path.join(root, name)
                    for root, dirs, files in os.walk(self.directory)
                    for name in files
                    if name.endswith((".xlsx", ".xls",'csv')) 
                        and all( x in name.lower() for x in  self.filter['include'] if x is not None) 
                        and all( x not in name.lower() for x in self.filter['exclude'] if x is not None) 
                        and all( x not in name.lower() for x in self.filter['broken'] if x is not None)                
                        ]                 
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

    def transform_reporting(directory,broken_files,drop_columns,reporting_columns,rename_columns,pluginlist=''):
        reporting_df = read_files(directory,broken_files,drop_columns,rename_columns) 
        ### reporting lines
        for col_list in reporting_columns:
            new_col = col_list[0]
            old_col = col_list[1]
            data_type_change = col_list[2]
            not_repurpose = col_list[3]
            ### date cleaning
            if data_type_change == 'date' and not_repurpose == 'repurpose' and old_col in reporting_df.columns:
                reporting_df[new_col] = reporting_df[old_col] 
                reporting_df[new_col] = pd.to_datetime(reporting_df[new_col], errors='coerce', dayfirst=True)
            ### time cleaning
            if data_type_change == 'time' and not_repurpose == 'repurpose' and old_col in reporting_df.columns:
                reporting_df[new_col] = reporting_df[old_col]
                reporting_df[new_col] = pd.to_datetime(reporting_df[new_col], format = '%H:%M', errors='coerce').datetime.time
                reporting_df[new_col].fillna(pd.to_datetime(reporting_df[old_col], format = '%H:%M:%S', errors='coerce').datetime.time, inplace=True)
                reporting_df[new_col].fillna(pd.to_datetime(reporting_df[old_col], format = '%Y%m%d %H:%M:%S', errors='coerce').datetime.time, inplace=True)
            ### string repurpose
            if data_type_change == 'string' and not_repurpose == 'repurpose' and old_col in reporting_df.columns:
                reporting_df[new_col] = reporting_df[old_col]
                reporting_df[new_col].fillna(0, inplace=True)
                try:
                    reporting_df[new_col] = reporting_df[new_col].astype(int)
                    reporting_df[new_col] = reporting_df[new_col].astype(str) 
                except ValueError:
                    reporting_df[new_col] = reporting_df[new_col].astype(str)      
            if data_type_change == 'string' and not_repurpose == 'new' and new_col == 'Reporting_Award' and old_col in reporting_df.columns:
                reporting_df[new_col] = 'GRIA' 
            #### work out monthly W/F/M 
            if data_type_change == 'date' and not_repurpose == 'new' and  new_col == 'Reporting_PeriodStartDate' and old_col in reporting_df.columns:
                reporting_df[new_col] = reporting_df[old_col] 
                reporting_df[new_col] = pd.to_datetime(reporting_df[new_col], errors='coerce', dayfirst=True)
                reporting_df[new_col] = reporting_df.apply(pp_dateset, axis = 1)
        try:
            if pluginlist is None:
                pass
            else:
                for i in pluginlist:
                    i(reporting_df)
        except ValueError:
            print('Plugin' + i + 'failed to run')                  
        return reporting_df




