from luigi.task import WrapperTask
from scripts.staging import *

class StagingPayslipsTests(Task):
    col=Parameter()
    test_content=Parameter()
    def output(self):
        path=os.path.join("./data/test/exceptions/"+str(self.col)+'_'+str(self.test_content)+'.csv')
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_csv('./data/staging/raw/payslips.csv')
        result=getattr(test.StagingTest(data),self.test_content)(self.col)
        with self.output().open('wb') as ofile:
            result.to_csv(ofile)
            del data,result

class StagingPayslipsExam(Task):
    params=DictParameter()

    def requires(self):
        return StagingRawTest()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_csv(self.params['input_file'])
        config_tests=[{i['staging']:i['tests']} for i in project_config['staging']['payslips']]
        tasks=[]
        for i in config_tests:
            for key,val in i.items():
                if val==[]:
                    tasks.append(StagingPayslipsTests(key,'nan_test'))
                else:
                    for k in val:
                        tasks.append(StagingPayslipsTests(key,k+'_test'))
        processed_file=yield tasks
        dataset=test.StagingTest(data).overview_test([{i['staging']:i['type']} for i in project_config['staging']['payslips']])
        with self.output().open('wb') as ofile:
            dataset.to_csv(ofile)
            del dataset

class StagingTimesheetsTests(Task):
    col=Parameter()
    test_content=Parameter()
    def output(self):
        path=os.path.join("./data/test/exceptions/"+str(self.col)+'_'+str(self.test_content)+'.csv')
        return LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_csv('./data/staging/raw/timesheets.csv')
        result=getattr(test.StagingTest(data),self.test_content)(self.col)
        with self.output().open('wb') as ofile:
            result.to_csv(ofile)
            del data,result

class StagingTimesheetsExam(Task):
    params=DictParameter()

    def requires(self):
        return StagingRawTest()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=pd.read_csv(self.params['input_file'])
        config_tests=[{i['staging']:i['tests']} for i in project_config['staging']['timesheets']]
        tasks=[]
        for i in config_tests:
            for key,val in i.items():
                if val==[]:
                    tasks.append(StagingTimesheetsTests(key,'nan_test'))
                else:
                    for k in val:
                        tasks.append(StagingTimesheetsTests(key,k+'_test'))
        processed_file=yield tasks
        dataset=test.StagingTest(data).overview_test([{i['staging']:i['type']} for i in project_config['staging']['timesheets']])
        with self.output().open('wb') as ofile:
            dataset.to_csv(ofile)
            del dataset

class GenerateExceptionReports(Task):
    params=DictParameter()

    def requires(self):
        return [StagingPayslipsExam(),StagingTimesheetsExam()]
    
    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        input_path=core.TargetFiles(self.params['input_folder']).folder_reader()

        with self.output().open('wb') as ofile:
            with pd.ExcelWriter(ofile) as writer: 
                for file in input_path:
                    df = pd.read_csv(file)
                    filename=os.path.basename(file)
                    if df.empty==False:
                        if filename=='payslips.csv':
                            df.to_excel(writer,sheet_name=filename)
                        elif filename=='timesheets.csv':
                            df.to_excel(writer,sheet_name=filename)
                        elif filename=='payslips_dups.csv':
                            df.to_excel(writer,sheet_name=filename)
                        elif filename=='timesheets_dups.csv':
                            df.to_excel(writer,sheet_name=filename)
                        else:
                            df.to_excel(writer,sheet_name=filename) 
               
            