from scripts.extract import *

class StagingPayslipsRaw(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        config_cols=[i['raw'] for i in project_config['staging']['payslips']]
        require_cols=[i['staging'] for i in project_config['staging']['payslips']]
        data=core.FileHandler(self.params['input_file']).read_staging()
        data=core.Transform(data).config_cols(config_cols,require_cols)
        config_plugins=[i['plugin'] for i in project_config['staging']['payslips']]
        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugins.Payslips(data), i['name'])(i)
        data=core.Transform(data).rename_cols(require_cols)
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(data)
            del data

class StagingTimesheetsRaw(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        config_cols=[i['raw'] for i in project_config['staging']['timesheets']]
        require_cols=[i['staging'] for i in project_config['staging']['timesheets']]
        data=core.FileHandler(self.params['input_file']).read_staging()
        data=core.Transform(data).config_cols(config_cols,require_cols)
        config_plugins=[i['plugin'] for i in project_config['staging']['timesheets']]

        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugins.Timesheets(data), i['name'])(i)
        data=core.Transform(data).rename_cols(require_cols)
        with self.output().open('wb') as ofile:
            core.FileHandler(ofile).save(data)
            del data
class StagingRawTest(luigi.Task):
    params=luigi.DictParameter()

    def requires(self):
        return [StagingPayslipsRaw(),StagingTimesheetsRaw()]
    
    def output(self):
        return luigi.LocalTarget(self.params['output_file'],format=luigi.format.Nop)

    def run(self):
        to_compare={
            'p_new': core.FileHandler(self.params['staging']+'/payslips.csv').read_staging(),
            'p_old': core.FileHandler(self.params['metaraw']+'/payslips.csv').read_staging(),
            't_new': core.FileHandler(self.params['staging']+'/timesheets.csv').read_staging(),
            't_old': core.FileHandler(self.params['metaraw']+'/timesheets.csv').read_staging()
        }
        results=test.RawTest(to_compare).missing_records_test()
        if isinstance(results,str):
            raise ValueError(results)
        else:
            with self.output().open('wb') as ofile:
                core.FileHandler(ofile).save(results)
                del results,to_compare
