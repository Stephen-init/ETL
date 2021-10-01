import pandas as pd
import numpy as np
import os
import dask.dataframe as dd
import uuid

#input
input_dir = r'C:\Users\steph\Yellow Canary\Yellow Canary - Documents\Clients\Projects\HSF\Sunflower\01_Data\02_Import Data\Cohort 1\Cohort 1\PaySlips'
dic={}
for filename in os.listdir(input_dir):
    if filename.endswith(".xlsx"):
        myfile=pd.read_excel(os.path.join(input_dir,filename))
        dic[filename]=myfile

all([set(dic[list(dic.keys())[0]].columns) == set(dic[df].columns) for df in dic])
payslip=pd.concat(d.values(), ignore_index=True)
payslip['row_source_id'] = [uuid.uuid4() for _ in range(len(payslip.index))]

payslip_pp = payslip[["IDNumber", "PeriodEndingDate"]]
payslip_pp = payslip_pp.drop_duplicates()
payslip_pp['payperiod_id'] = [uuid.uuid4() for _ in range(len(payslip_pp.index))]
payslip = (pd.merge(payslip, payslip_pp, left_on = ['IDNumber','PeriodEndingDate'], right_on=['IDNumber','PeriodEndingDate'], how='left'))

