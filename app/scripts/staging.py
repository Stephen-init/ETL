from scripts.extract import *
import scripts.plugins as plugins

class StagingPayslipsRaw(Task):
    params=DictParameter()

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        config_columns=[i['raw'] for i in project_config['staging']['payslips']]
        data=pd.read_csv(self.params['input_file'])
        data = data.reindex(data.columns.union(config_columns, sort=False), axis=1)
        data= data[config_columns]
        data.columns=[i['staging'] for i in project_config['staging']['payslips']]
        config_plugins=[i['plugin'] for i in project_config['staging']['payslips']]

        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugins.Payslips(data), i['name'])(i)

        data=data[[i['staging'] for i in project_config['staging']['payslips']]]
        with self.output().open('wb') as ofile:
            data.to_csv(ofile, index=False)
            del data



class StagingTimesheetsRaw(Task):
    params=DictParameter()

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)
    
    def run(self):
        config_columns=[i['raw'] for i in project_config['staging']['timesheets']]
        data=pd.read_csv(self.params['input_file'])
        data = data.reindex(data.columns.union(config_columns, sort=False), axis=1)
        data= data[config_columns]
        data.columns=[i['staging'] for i in project_config['staging']['timesheets']]
        config_plugins=[i['plugin'] for i in project_config['staging']['timesheets']]

        for i in config_plugins:
            if i!={}:
                if i['switch']==True:
                    data=getattr(plugins.Timesheets(data), i['name'])(i)
        data=data[[i['staging'] for i in project_config['staging']['timesheets']]]
        with self.output().open('wb') as ofile:
            data.to_csv(ofile, index=False)
            del data



class StagingRawTest(Task):
    params=DictParameter()

    def requires(self):
        return [StagingPayslipsRaw(),StagingTimesheetsRaw()]
    
    def output(self):
        return LocalTarget(self.params['output_file'],format=luigi.format.Nop)

    def run(self):
        p_staging=test.StagingTest({'p_staging':pd.read_csv(self.params['staging']+'/payslips.csv')}).missing_records_test()
        p_metaraw=test.StagingTest({'p_metaraw':pd.read_csv(self.params['metaraw']+'/payslips.csv')}).missing_records_test()
        t_staging=test.StagingTest({'t_staging':pd.read_csv(self.params['staging']+'/timesheets.csv')}).missing_records_test()
        t_metaraw=test.StagingTest({'t_metaraw':pd.read_csv(self.params['metaraw']+'/timesheets.csv')}).missing_records_test()
        if p_staging!=p_metaraw or t_staging!=t_metaraw:
            message_list=['Missing Records have been found:',
                          'Payslips:', 
                          'Staging has {} records', 
                          'Metaraw has {} records',
                          'Timesheets:', 
                          'Staging has {} records', 
                          'Metaraw has {} records'
                        ]
            message='\n'.join(message_list)
            raise ValueError(message.format(p_staging,p_metaraw,t_staging,t_metaraw))
        else:
            dataset=pd.Series({'Payslips Staging':p_staging, 'Payslips Metaraw':p_metaraw,
                                  'Timesheets Staging':t_staging, 'Timesheets Metaraw':t_metaraw,
                                })
            with self.output().open('wb') as ofile:
                dataset.to_csv(ofile)
                del dataset

