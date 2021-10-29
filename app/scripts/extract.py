import luigi
from yclib import core,test,plugins

global project_config
project_config=core.FileHandler("project.yaml").read_config()

def broken_file(data):
    f= open(".data/raw/broken.txt","a+")
    f.write(data+"/n")
    f.close()
class PayslipsRaw(luigi.Task):
    filepath=luigi.Parameter()
    index=luigi.IntParameter()
    def output(self):
        path=core.DirHandler().path_join("data/raw/payslips","dataset"+str(self.index)+'.csv')
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler(self.filepath).read_raw()
        with self.output().open('wb') as ofile:
            if isinstance(data,str):
                broken_file(data)
            else:
                core.FileHandler(ofile).save(data)
            del data

class PayslipsMeta(luigi.Task):
    params=luigi.DictParameter()
    def output(self):
        path=core.DirHandler().path_join(self.params['output_folder'],self.params['output_file'])
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        input_path=core.DirHandler(self.params['input_raw'],project_config['raw']['payslips']).folder_reader()
        counter=1
        tasks=[]
        for file in input_path:
            tasks.append(PayslipsRaw(file,counter))
            self.set_progress_percentage(100*counter/len(input_path))
            counter=counter+1
        processed_file=yield tasks
        alldf=[]
        for file in core.DirHandler(self.params['input_folder']).folder_reader():
            df=core.FileHandler(file).read_staging()
            alldf.append(df)
        dataset=core.Transform([i for i in alldf]).concat_dfs()
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(dataset)
            del dataset

class PayslipsMetaColumnTest(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return PayslipsMeta()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        files=core.DirHandler(self.params['input_folder']).folder_reader()
        data=core.FileHandler(files).read_staging()
        coltest=test.RawTest(data).column_test()
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(coltest)
            del data,coltest

class TimesheetsRaw(luigi.Task):
    filepath=luigi.Parameter()
    index=luigi.IntParameter()
    def output(self):
        path=core.DirHandler().path_join("data/raw/timesheets","dataset"+str(self.index)+'.csv')
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        data=core.FileHandler(self.filepath).read_raw()
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(data)
            del data

class TimesheetsMeta(luigi.Task):
    params=luigi.DictParameter()
    def output(self):
        path=core.DirHandler().path_join(self.params['output_folder'],self.params['output_file'])
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        input_path=core.DirHandler(self.params['input_raw'],project_config['raw']['timesheets']).folder_reader()
        counter=1
        tasks=[]
        for file in input_path:
            tasks.append(TimesheetsRaw(file,counter))
            self.set_progress_percentage(100*counter/len(input_path))
            counter=counter+1
        processed_file=yield tasks
        alldf=[]
        for file in core.DirHandler(self.params['input_folder']).folder_reader():
            df=core.FileHandler(file).read_staging()
            alldf.append(df)
        dataset=core.Transform([i for i in alldf]).concat_dfs()
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(dataset)
            del dataset

class TimesheetsMetaColumnTest(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return TimesheetsMeta()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        files=core.DirHandler(self.params['input_folder']).folder_reader()
        data=core.FileHandler(files).read_staging()
        coltest=test.RawTest(data).column_test()
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(coltest)
            del data,coltest

class ExtractTest(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return [PayslipsMetaColumnTest(),TimesheetsMetaColumnTest()]
    
    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        to_compare={
            'p_new':core.FileHandler(self.params['metaraw']+'/payslips.csv').read_staging(),
            'p_old': [core.FileHandler(i).read_staging() for i in core.DirHandler(self.params['payslips']).folder_reader()],
            't_new': core.FileHandler(self.params['metaraw']+'/timesheets.csv').read_staging(),
            't_old':[core.FileHandler(i).read_staging() for i in core.DirHandler(self.params['timesheets']).folder_reader()],
        }
        results=test.RawTest(to_compare).missing_records_test()
        if isinstance(results,str):
            raise ValueError(results)
        else:
            with self.output().open('wb') as ofile:
                core.FileHandler(ofile).save(results)
                del results,to_compare

class RemoveDuplicates(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return ExtractTest()
    
    def output(self):
        return {'payslips_dups': luigi.LocalTarget(self.params['payslips_dups'],format=luigi.format.Nop),
                'timesheets_dups': luigi.LocalTarget(self.params['timesheets_dups'],format=luigi.format.Nop),
                'payslips': luigi.LocalTarget(self.params['payslips'],format=luigi.format.Nop),
                'timesheets': luigi.LocalTarget(self.params['timesheets'],format=luigi.format.Nop),
                }

    def run(self):
        meta_payslips=core.FileHandler(self.params['metaraw_payslips']).read_staging()
        meta_timesheets=core.FileHandler(self.params['metaraw_timesheets']).read_staging()
        cleand_payslips=core.Transform(meta_payslips).duplicates()
        cleand_timesheets=core.Transform(meta_timesheets).duplicates()
        
        with self.output()['payslips_dups'].open('wb') as ofile:
            core.FileHandler(ofile).save(cleand_payslips[1])
        with self.output()['payslips'].open('wb') as ofile:
            core.FileHandler(ofile).save(cleand_payslips[0])
        with self.output()['timesheets_dups'].open('wb') as ofile:
            core.FileHandler(ofile).save(cleand_timesheets[1])
        with self.output()['timesheets'].open('wb') as ofile:
            core.FileHandler(ofile).save(cleand_timesheets[0])
        del meta_payslips,meta_timesheets,cleand_payslips,cleand_timesheets
