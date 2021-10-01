user={
        'stephen':{'raw': r"/Users/stephen/Yellow Canary/Yellow Canary - Documents/Clients/Projects/HSF/Sunflower/01_Data",
                   'test': r'E:\stephen-workspace\Tests',
                   'exception': r'C:\Users\steph\Yellow Canary\Yellow Canary - Documents\Clients\Projects\HSF\Sunflower\01_Data',
                   'staging': r'C:\Users\steph\Yellow Canary\Yellow Canary - Documents\Clients\Projects\HSF\Sunflower\01_Data',
                   'final': r'C:\Users\steph\Yellow Canary\Yellow Canary - Documents\Clients\Projects\HSF\Sunflower\01_Data'
                   }

}


payslip={
    'files':{
            'include':['payslip','cohort'],
            'exclude':['sample','sunflower'],
            'broken':[]
            },
    'columns' : {
                'include':[],
                'drop': [],
                'add':[],
                'rename':{
                        'HoursorADCodeDesc': ['HoursorA/DCodeDesc']
                        ,'HoursorADCode': ['HoursorA/DCode']
                }},
    'staging' : [
                ['Reporting_EmployeeCode','IDNumber','string']
                ,['Reporting_PayslipCode','payperiod_id','string']
                ,['Reporting_PaysliplineCode','row_source_id','string']
                ,['Reporting_PeriodStartDate','PeriodEndingDate','date']
                ,['Reporting_PeriodEndDate','PeriodEndingDate','date']
                ,['Reporting_Paycode','HoursorADCodeDesc','string']
                ,['Reporting_Hours','NumberofUnits','float']
                ,['Reporting_Amount','Value','float']
                ,['Reporting_Classification','HoursorADCodeDesc','string']
                ,['Reporting_EmployeeType','EmploymentTypeCodeDesc','string']
                ,['Reporting_EmployeeClassication','JobTitleCodeDescription','string']
                ,['Reporting_DateOfBirth','DateofBirth','date']
                ,['Reporting_BaseHours','BaseHours','string']
                ,['Reporting_Source','file_name','string']
                ,['Reporting_Site','AccountCodeDescription','string']
                ]
        }

timesheets={
    'files':{
            'include':['timesheet','cohort'],
            'exclude':['sample'],
            'broken':[]
            },
    'columns' : {
                'include':[],
                'drop': ['pay_cd','pay_description','pay_category'
                            ,'modified_classification_cd','class_description'
                            ,'SalaryCode','Unnamed:25','Unnamed:27'
                            ,'FirstName','LastName'],
                'add':[],
                'rename':{
                        'EmployeeID': ['EpmloyeeID','EmployeeId']
                }},
    'staging' : [
                ['Reporting_EmployeeCode','ExportCode','string','repurpose']
                ,['Reporting_Award','String','string','new']
                ,['Reporting_Date','Date','date','repurpose']
                ,['Reporting_ClockedIn','PayStart','time','repurpose']
                ,['Reporting_ClockedOut','PayEnd','time','repurpose']
                ,['Reporting_RosteredIn','SchedStart','time','repurpose']
                ,['Reporting_RosteredOut','SchedEnd','time','repurpose']
                ,['Reporting_Break','Break','time','repurpose']
                ,['Reporting_Region','Site','string','repurpose']
                ,['Reporting_ShiftRole','Role','string','repurpose']
                ,['Reporting_ShiftClassification','EmployeeType','string','repurpose']
                    ]
        }

tests={
    'files':{
            'include':['dataset'],
            'exclude':['sample','sunflower'],
            'broken':[]
            },
    'columns' : {
                'include':[],
                'drop': [],
                'add':[],
                'rename':{
                        'HoursorADCodeDesc': ['HoursorA/DCodeDesc']
                        ,'HoursorADCode': ['HoursorA/DCode']
                }},
    'staging' : [
                ['Reporting_EmployeeCode','IDNumber','string']
                ,['Reporting_PayslipCode','payperiod_id','string']
                ,['Reporting_PaysliplineCode','row_source_id','string']
                ,['Reporting_PeriodStartDate','PeriodEndingDate','date']
                ,['Reporting_PeriodEndDate','PeriodEndingDate','date']
                ,['Reporting_Paycode','HoursorADCodeDesc','string']
                ,['Reporting_Hours','NumberofUnits','float']
                ,['Reporting_Amount','Value','float']
                ,['Reporting_Classification','HoursorADCodeDesc','string']
                ,['Reporting_EmployeeType','EmploymentTypeCodeDesc','string']
                ,['Reporting_EmployeeClassication','JobTitleCodeDescription','string']
                ,['Reporting_DateOfBirth','DateofBirth','date']
                ,['Reporting_BaseHours','BaseHours','string']
                ,['Reporting_Source','file_name','string']
                ,['Reporting_Site','AccountCodeDescription','string']
                ]
        }

final_report={
                'configuration': 'ClientId',
                'EmployeeBaseFile':[
                                            ['EmployeeCode', 'string']
                                            ,['StartDate','time']
                                            ,['EndDate','time']
                                            ,['DateOfBirth','time']
                                            ,['Source','string']
                                    ],
                'EmployeeClassifications':[
                                            ['EmployeeCode', 'string']
                                            ,['StartDate','time']
                                            ,['EndDate','time']
                                            ,['Classification','string'] #employee level
                                        ],  
                'EmployeeEmploymentTypes':[
                                            ['EmployeeCode', 'string']
                                            ,['StartDate','time']
                                            ,['EndDate','time']
                                            ,['EmploymentType','string'] # Casual,FullTime,PartTime
                                        ],  
                'Leave':[ #supports additional columns
                                            ['EmployeeCode', 'string']
                                            ,['Start','time']
                                            ,['End','time']
                                            ,['PayCode','string'] #PaidLeave, AnnualLeave
                                            ,['Source','string']
                        ],     
                'Payslips':[ #supports additional columns
                                            ['EmployeeCode', 'string']
                                            ,['PayslipCode','string'] #UUID
                                            ,['PayslipLineCode','string'] #UUID
                                            ,['PeriodStart','time'] #yyyy-mm-ddT00:00:00
                                            ,['PeriodEnd','time'] #yyyy-mm-ddT00:00:00
                                            ,['PayCode','string'] #Basepay / After6am / Holiday / OT250 etc
                                            ,['Description','string'] #PayCode description
                                            ,['Hours','Numeric']
                                            ,['Amount','Numeric']
                                            ,['Classification', 'string']  #  ['Ordinary','Overtime','Penalty','CasualLoading','Leave','Allowance']
                                            ,['Source','string']
                                ],
                'Timesheets':[ #supports additional columns
                                            ['EmployeeCode', 'string']
                                            ,['Award','string'] # national award retail / GIRA / according to law
                                            ,['ClockedIn','time'] #actural working
                                            ,['ClockedOut','time'] #actural working
                                            ,['RosteredIn','time'] #should have working
                                            ,['RosteredOut','time'] #should have working
                                            ,['UnpaidBreaks','string'] #2019-01-02T11:00:00=>2019-01-02T11:30:00&2019-01-02T15:30:00=>2019-01-02T14:00:00
                                            ,['Region','string'] #National,Qld,Nsw,Act,Vic,Tas,Nt,Sa,Wa,Brisbane
                                            ,['Source','string']
                        ],                  
            
            }

plugins={
    'TransformReportingSup' : {
        'generate_payslip_code': {
                                  'apply':True, 
                                  'fiters':["IDNumber", "PeriodEndingDate"]
                                  },
        'shift_reporting_period': {
                                  'apply':True, 
                                  'ref_col':'PayFrequency',
                                  'ref_value':{'W':6,'F':13,'M':'start'} #for month, set to start
                                  }
        }

}
