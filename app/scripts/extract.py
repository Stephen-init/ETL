import luigi,os,yaml
from luigi.task import WrapperTask
from luigi import Task,Target,Parameter,LocalTarget,IntParameter,DictParameter,ListParameter,WrapperTask
import pandas as pd
import scripts.test as test
import scripts.core as core

global project_config
with open("project.yaml","r") as f:
    project_config=yaml.load(f,Loader=yaml.CLoader)

class PayslipsRaw(Task):
    filepath=Parameter()
    index=IntParameter()
    def output(self):
        path=os.path.join("./data/raw/payslips/dataset"+str(self.index)+'.csv')
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.ReadFiles(self.filepath).read_files()
        with self.output().open('wb') as ofile:
            data.to_csv(ofile,index=False)
            del data

class PayslipsMeta(Task):
    params=DictParameter()
    def output(self):
        path=os.path.join(self.params['output_folder'],self.params['output_file'])
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        input_path=core.TargetFiles(self.params['input_raw']).folder_reader(filter=project_config['raw']['payslips'])
        counter=1
        tasks=[]
        for file in input_path:
            tasks.append(PayslipsRaw(file,counter))
            self.set_progress_percentage(100*counter/len(input_path))
            counter=counter+1
        processed_file=yield tasks
        alldf=[]
        for file in os.listdir(self.params['input_folder']):
            df=pd.read_csv(os.path.join(self.params['input_folder'],file))
            alldf.append(df)
        dataset=pd.concat([i for i in alldf])
        with self.output().open('wb') as ofile:
            dataset.to_csv(ofile,index=False)
            del dataset

class PayslipsMetaColumnTest(Task):
    params=DictParameter()

    def requires(self):
        return PayslipsMeta()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=core.TargetFiles(self.params['input_folder']).read_files()
        coltest=test.RawTest(data).column_test()
        with self.output().open('wb') as ofile:
            coltest.to_csv(ofile)
            del data,coltest

class TimesheetsRaw(Task):
    filepath=Parameter()
    index=IntParameter()
    def output(self):
        path=os.path.join("./data/raw/timesheets/dataset"+str(self.index)+'.csv')
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.ReadFiles(self.filepath).read_files()
        with self.output().open('wb') as ofile:
            data.to_csv(ofile,index=False)
            del data

class TimesheetsMeta(Task):
    params=DictParameter()
    def output(self):
        path=os.path.join(self.params['output_folder'],self.params['output_file'])
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        input_path=core.TargetFiles(self.params['input_raw']).folder_reader(project_config['raw']['timesheets'])
        counter=1
        tasks=[]
        for file in input_path:
            tasks.append(TimesheetsRaw(file,counter))
            self.set_progress_percentage(100*counter/len(input_path))
            counter=counter+1
        processed_file=yield tasks
        alldf=[]
        for file in os.listdir(self.params['input_folder']):
            df=pd.read_csv(os.path.join(self.params['input_folder'],file))
            alldf.append(df)
        dataset=pd.concat([i for i in alldf],ignore_index=True)
        with self.output().open('wb') as ofile:
            dataset.to_csv(ofile,index=False)
            del dataset

class TimesheetsMetaColumnTest(Task):
    params=DictParameter()
    def requires(self):
        return TimesheetsMeta()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=core.TargetFiles(self.params['input_folder']).read_files()
        coltest=test.RawTest(data).column_test()
        with self.output().open('wb') as ofile:
            coltest.to_csv(ofile)
            del data,coltest

class ExtractTest(Task):
    params=DictParameter()

    def requires(self):
        return [PayslipsMetaColumnTest(),TimesheetsMetaColumnTest()]
    
    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        p_metaraw=test.RawTest({'p_metaraw':pd.read_csv(self.params['metaraw']+'/payslips.csv')}).missing_records_test()
        p_path=[os.path.join(root, name) for root, dirs, files in os.walk(self.params['payslips']) for name in files]
        p_rawdata=test.RawTest(dict(zip(p_path,[pd.read_csv(i) for i in p_path]))).missing_records_test()
        t_metaraw=test.RawTest({'t_metaraw':pd.read_csv(self.params['metaraw']+'/timesheets.csv')}).missing_records_test()
        t_path=[os.path.join(root, name) for root, dirs, files in os.walk(self.params['timesheets']) for name in files]
        t_rawdata=test.RawTest(dict(zip(t_path,[pd.read_csv(i) for i in t_path]))).missing_records_test()
        if p_metaraw!=p_rawdata or t_metaraw!=t_rawdata:
            message_list=['Missing Records have been found:',
                          'Payslips:', 
                          'Metaraw has {} records', 
                          'raw files have {} records',
                          'Timesheets:', 
                          'Metaraw has {} records', 
                          'raw files have {} records'
                        ]
            message='\n'.join(message_list)
            raise ValueError(message.format(p_metaraw,p_rawdata,t_metaraw,t_rawdata))
        else:
            dataset=pd.Series({'Payslips Metaraw':p_metaraw, 'Payslips Raw Files':p_rawdata,
                                  'Timesheets Metaraw':t_metaraw, 'Timesheets Raw Files':t_rawdata,
                                })
            with self.output().open('wb') as ofile:
                dataset.to_csv(ofile)
                del dataset,p_metaraw,p_path,p_rawdata,t_metaraw,t_path,t_rawdata
class RemoveDuplicates(Task):
    params=DictParameter()

    def requires(self):
        return ExtractTest()
    
    def output(self):
        return {'payslips_dups': LocalTarget(self.params['payslips_dups'],format=luigi.format.Nop),
                'timesheets_dups': LocalTarget(self.params['timesheets_dups'],format=luigi.format.Nop),
                'payslips': LocalTarget(self.params['payslips'],format=luigi.format.Nop),
                'timesheets': LocalTarget(self.params['timesheets'],format=luigi.format.Nop),
                }

    def run(self):
        meta_payslips=pd.read_csv(self.params['metaraw_payslips'])
        meta_timesheets=pd.read_csv(self.params['metaraw_timesheets'])
        cleand_payslips=core.cleaning(meta_payslips).duplicates()
        cleand_timesheets=core.cleaning(meta_timesheets).duplicates()
        
        with self.output()['payslips_dups'].open('wb') as ofile:
            cleand_payslips[1].to_csv(ofile)
        with self.output()['payslips'].open('wb') as ofile:
            cleand_payslips[0].to_csv(ofile)
        with self.output()['timesheets_dups'].open('wb') as ofile:
            cleand_timesheets[1].to_csv(ofile)
        with self.output()['timesheets'].open('wb') as ofile:
            cleand_timesheets[0].to_csv(ofile)
        del meta_payslips,meta_timesheets,cleand_payslips,cleand_timesheets
