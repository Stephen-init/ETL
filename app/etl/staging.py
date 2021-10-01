from numpy import datetime64
import pandas as pd
from datetime import datetime

class FinalReport:
    def __init__(self,project:str,report:list,data:list):
        self.snapshotdate = datetime.now().strftime("%Y%m%d-%H%M")
        self.project=project
        self.report=report
        self.data=data

    def GenerateReport(self):
        df=pd.DataFrame(
                data=[self.data], columns=['ClientId']
                )
        df=df.set_index('ClientId')
        df.to_csv(r'E:\stephen-workspace\Reports\{:s}.csv'.format(data))
    def EmployeeBaseFile(self):
        data=[self.EmployeeCode,self.StartDate,self.EndDate,self.DateOfBirth,self.Source]
        df=pd.DataFrame(
                data=data, columns=['ClientId']
                )
        df=df.set_index(self.EmployeeCode)
        df.to_csv(r'E:\stephen-workspace\Reports\{:s}-{:s}-EmployeeClassification.csv'.format(data,self.snapshotdate))