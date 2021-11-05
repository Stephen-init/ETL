import luigi
from yclib import reader,merger,plugin,tester


global project_config
project_config=reader.read_config("project.yaml")


class StagingPayslipsRaw(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/staging/raw/payslips.csv",format=luigi.format.Nop)
    
    def run(self):
        config_cols=[i['raw'] for i in project_config['staging']['payslips']]
        require_cols=[i['staging'] for i in project_config['staging']['payslips']]
        data=reader.read_staging("data/metaraw/dupfree/payslips.csv")
        data=merger.Transform(data).config_cols(config_cols,require_cols)
        config_plugins=[i['plugin'] for i in project_config['staging']['payslips']]
        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugin.Payslips(data), i['name'])(i)
        data=merger.Transform(data).rename_cols(require_cols)
        with self.output().open('wb') as ofile:
            reader.save(ofile,data)
            del data

class StagingTimesheetsRaw(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/staging/raw/timesheets.csv",format=luigi.format.Nop)
    
    def run(self):
        config_cols=[i['raw'] for i in project_config['staging']['timesheets']]
        require_cols=[i['staging'] for i in project_config['staging']['timesheets']]
        data=reader.read_staging("data/metaraw/dupfree/timesheets.csv")
        data=merger.Transform(data).config_cols(config_cols,require_cols)
        config_plugins=[i['plugin'] for i in project_config['staging']['timesheets']]

        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugin.Timesheets(data), i['name'])(i)
        data=merger.Transform(data).rename_cols(require_cols)
        with self.output().open('wb') as ofile:
            reader.save(ofile,data)
            del data

class StagingRawTest(luigi.Task):

    def requires(self):
        return [StagingPayslipsRaw(),StagingTimesheetsRaw()]
    
    def output(self):
        return luigi.LocalTarget("data/test/process/staging_raw_test.csv",format=luigi.format.Nop)

    def run(self):
        to_compare={
            'p_new': reader.read_staging('data/staging/raw/payslips.csv'),
            'p_old': reader.read_staging('data/metaraw/dupfree/payslips.csv'),
            't_new': reader.read_staging('data/staging/raw/timesheets.csv'),
            't_old': reader.read_staging('data/metaraw/dupfree/timesheets.csv')
        }
        results=tester.RawTest(to_compare).missing_records_test()
        if isinstance(results,str):
            raise ValueError(results)
        else:
            with self.output().open('wb') as ofile:
                reader.save(ofile,results)
                del results,to_compare
