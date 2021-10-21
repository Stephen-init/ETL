import modin.pandas as pd
import uuid

class Payslips:
    def __init__(self,df:pd.DataFrame):
        self.df=df
    
    def PayslipCode(self,params):
        self.df[params['name']] = [uuid.uuid4() for _ in range(len(self.df.index))]
        return self.df
    
    def PaysliplineCode(self,params):
        p_code = self.df[params['ref']].drop_duplicates()
        p_code[params['name']] = [uuid.uuid4() for _ in range(len(p_code.index))]
        self.df = pd.merge(self.df, p_code, on=params['ref'],how='left',suffixes=('drop', ''))
        return self.df
    
    def PeriodStartDate(self,params):
        self.df.loc[self.df[params['ref'][0]]=='W',params['name']]= pd.to_datetime(self.df.loc[self.df[params['ref'][0]]=='W',params['ref'][1]]) - pd.DateOffset(days=6)
        self.df.loc[self.df[params['ref'][0]]=='F',params['name']]= pd.to_datetime(self.df.loc[self.df[params['ref'][0]]=='F',params['ref'][1]]) - pd.DateOffset(days=13)
        self.df.loc[self.df[params['ref'][0]]=='M',params['name']]= pd.to_datetime(self.df.loc[self.df[params['ref'][0]]=='M',params['ref'][1]]).to_numpy().astype('datetime64[M]') 
        return self.df

class Timesheets:
    def __init__(self,df:pd.DataFrame):
        self.df=df

    def Award(self,params):
        self.df[params['name']]='GRIA'
        return self.df
    