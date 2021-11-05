import luigi
from yclib import reader,tester,merger
import luigi.contrib.postgres

global project_config
project_config=reader.read_config("project.yaml")

class PayslipsMeta(luigi.Task):

    def output(self):
        return(luigi.contrib.postgres.PostgresTarget('postgres','luigi','analyst','temp','payslips_meta_raw','1','5432'))
    
    def run(self):
        file_lists=reader.folder_reader("sharedata",project_config['raw']['payslips'])
        dfs=reader.multi_read(reader.read_raw,file_lists)
        working_dfs=[reader.save_txt("data/exceptions/broken.txt",i) if isinstance(i,str) else i for i in dfs]
        dataset=merger.Transform(working_dfs).concat_dfs()
        coltest=tester.RawTest(working_dfs).column_test()
        reader.save("data/exceptions/payslips_column_test.csv",coltest)
        cleand_payslips=merger.Transform(dataset).duplicates()
        reader.save("data/exceptions/payslips_dups.csv",cleand_payslips[1])
        reader.save_json("data/tests/data_loss_test.json",{'payslips_meta_raw':tester.RawTest(cleand_payslips[0]).data_loss_test()})
        reader.save_postgres(cleand_payslips[0],"payslips_meta_raw","replace")
        del dfs,dataset,coltest,cleand_payslips,working_dfs

class TimesheetsMeta(luigi.Task):

    def output(self):
        return(luigi.contrib.postgres.PostgresTarget('postgres','luigi','analyst','temp','timesheets_meta_raw','1','5432'))
    
    def run(self):
        file_lists=reader.folder_reader("sharedata",project_config['raw']['timesheets'])
        dfs=reader.multi_read(reader.read_raw,file_lists)
        working_dfs=[reader.save_txt("data/exceptions/broken.txt",i) if isinstance(i,str) else i for i in dfs]
        dataset=merger.Transform(dfs).concat_dfs()
        coltest=tester.RawTest(working_dfs).column_test()
        reader.save("data/exceptions/timesheets_column_test.csv",coltest)
        cleand_timesheets=merger.Transform(dataset).duplicates()
        reader.save("data/exceptions/timesheets_dups.csv",cleand_timesheets[1])
        reader.save_json("data/tests/data_loss_test.json",{'timesheets_meta_raw':tester.RawTest(cleand_timesheets[0]).data_loss_test()})
        reader.save_postgres(cleand_timesheets[0],"timesheets_meta_raw","replace")
        del dfs,dataset,coltest,cleand_timesheets,working_dfs

class ExtractTest(luigi.Task):
    
    def requires(self):
        return [PayslipsMeta(),TimesheetsMeta()]
    
    def output(self):
        path="data/tests/data_loss_test_extract.json"
        return luigi.LocalTarget(path,format=luigi.format.Nop)
    
    def run(self):
        raw=reader.read_json("data/tests/data_loss_test.json")
        reader.remove_file("data/tests/data_loss_test.json")
        raw['payslips_meta_raw_postgres']=reader.read_postgres(
                                            'select count(*) from  payslips_meta_raw',
                                            'localhost','luigi','analyst','temp','str' )
        raw['timesheets_meta_raw_postgres']=reader.read_postgres(
                                            'select count(*) from  timesheets_meta_raw',
                                            'localhost','luigi','analyst','temp','str')
        if raw['payslips_meta_raw']==raw['payslips_meta_raw_postgres'] and raw['timesheets_meta_raw']==raw['timesheets_meta_raw_postgres']:
            with self.output().open('wb') as ofile:
                reader.save_json(ofile,raw)
            
        else:
            message_list=['Lossing data has been found:',
                          'Payslips:', 
                          'source files have {} records', 
                          'postgres table has {} records',
                          'Timesheets:', 
                          'source files have {} records', 
                          'postgres table has {} records'
                        ]
            results='\n'.join(message_list).format(raw['payslips_meta_raw'],raw['payslips_meta_raw_postgres'] , raw['timesheets_meta_raw'],raw['timesheets_meta_raw_postgres'])
            raise ValueError(results)


