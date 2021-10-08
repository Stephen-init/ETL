import luigi
from luigi import Task,Target,Parameter,LocalTarget,IntParameter,DictParameter
import pandas as pd
from core import *
from test import *
import os,yaml

global project_config
with open("project.yaml","r") as f:
    project_config=yaml.load(f,loader=yaml.CLoader)


class ReadRaw(Task):
    filename=Parameter()
    index=IntParameter()
    def output(self):
        path=os.path.join("./data/raw/dataset"+str(self.index)+'.csv')
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_excel(self.filename)
        data['source']=self.filename.split('/')[-1]
        with self.output().open('wb') as ofile:
            data.to_csv(ofile)

class GroupData(Task):
    params=DictParameter()

    def output(self):
        path=os.path.join(self.params['output_folder'],self.params['output_file'])
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        input_path=Raw(self.params['input_raw'],project_config['raw']['payslips']['files']).folder_reader()
        counter=1
        for file in input_path:
            target=yield ReadRaw(file,counter)
            counter=counter+1
        alldf=[]
        for file in os.listdir(self.params['input_folder']):
            df=pd.read_csv(os.path.join(self.params['input_folder'],file),encoding= 'unicode_escape')
            alldf.append(df)
        dataset=pd.concat([i for i in alldf])
        with self.output().open('wb') as ofile:
            dataset.to_csv(ofile)

class TestData(Task):
    params=DictParameter()

    def requires(self):
        return GroupData()

    def output(self):
        path=os.path.join(os.path.join(self.params['output_folder'],self.params['output_file']))
        return LocalTarget(path,format=luigi.format.Nop)

    def run(self):
        filters={
            'include':["dataset"],
            'exclude':[],
            'broken':[]
        }

        data=Raw(self.params['input_folder'],filters).read_files()
        coltest=RawTest(data).column_test(data)
        valuetest=RawTest(data).missing_values_table()

        with self.output().open('wb') as ofile:
            with pd.ExcelWriter(ofile) as writer:  # doctest: +SKIP
                coltest.to_excel(writer,sheet_name='Column Test')
                valuetest.to_excel(writer,sheet_name='Value Test')

class StagingTimesheets(Task):
    params=DictParameter()

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_csv(self.params['input_file'])
        data = data.reindex(data.columns.union(project_config['staging']['timesheets']['match'], sort=False), axis=1)
        for i,j in zip(list(data[project_config['staging']['timesheets']['match']].columns),project_config['staging']['timesheets']['type']):
            data[i]=data[i].astype(j,errors='ignore')
        for i in project_config['staging']['timesheets']['plugins']:
            if project_config['staging']['timesheets']['plugins'][i]['switch']==True:
                data=getattr(plugins.timesheets(data), i)(project_config['staging']['timesheets']['plugins'][i])
        data= data[project_config['staging']['timesheets']['match']]
        data.columns=project_config['staging']['timesheets']['requires']
        with self.output().open('wb') as ofile:
            data.to_csv(ofile, index=False)
            del data
