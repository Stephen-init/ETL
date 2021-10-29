from scripts.staging import *

class StagingPayslipsTests(luigi.Task):
    col=luigi.Parameter()
    test_content=luigi.Parameter()
    def output(self):
        path=core.DirHandler().path_join("./data/test/exceptions/"+str(self.col)+'_'+str(self.test_content)+'.csv')
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler('./data/staging/raw/payslips.csv').read_staging()
        result=getattr(test.StagingTest(data),self.test_content)(self.col)
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(result)
            del data,result

class StagingPayslipsExam(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return StagingRawTest()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler(self.params['input_file']).read_staging()
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
            core.FileHandler(ofile).save(dataset)
            del dataset

class StagingTimesheetsTests(luigi.Task):
    col=luigi.Parameter()
    test_content=luigi.Parameter()
    def output(self):
        path=core.DirHandler().path_join("./data/test/exceptions/"+str(self.col)+'_'+str(self.test_content)+'.csv')
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler('./data/staging/raw/timesheets.csv').read_staging()
        result=getattr(test.StagingTest(data),self.test_content)(self.col)
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(result)
            del data,result

class StagingTimesheetsExam(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return StagingRawTest()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler(self.params['input_file']).read_staging()
        config_tests=[{i['staging']:i['tests']} for i in project_config['staging']['timesheets']]
        tasks=[]
        for i in config_tests:
            for key,val in i.items():
                if val==[]:
                    tasks.append(StagingPayslipsTests(key,'nan_test'))
                else:
                    for k in val:
                        tasks.append(StagingPayslipsTests(key,k+'_test'))
        processed_file=yield tasks
        dataset=test.StagingTest(data).overview_test([{i['staging']:i['type']} for i in project_config['staging']['timesheets']])
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(dataset)
            del dataset

class GenerateExceptionReports(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return [StagingPayslipsExam(),StagingTimesheetsExam()]
    
    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        input_path=core.DirHandler(self.params['input_folder']).folder_reader()

        with self.output().open('wb') as ofile:
            core.FileHandler(input_path).save_excel(ofile)
            