import pandas as pd
import uuid,datetime
class TransformReportingSup:
    def __init__ (self,df:pd.DataFrame):
        self.df=df

    def generate_payslip_code(self,filters=list): #for reports don't have PayslipCode & PaysliplineCode
        self.df['Reporting_PaysliplineCode'] = [uuid.uuid4() for _ in range(len(self.df.index))]
        df_pp = self.df[filters]
        df_pp = df_pp.drop_duplicates()
        df_pp['Reporting_PayslipCode'] = [uuid.uuid4() for _ in range(len(df_pp.index))]
        self.df = pd.merge(self.df, self.df_pp, 
                            left_on = filters, 
                            right_on=filters, 
                            how='left')
        return self.df
                    
    def shift_reporting_period(self,ref_col,ref_value:dict): #used to change periodstartdate according to PayFrequency
        for key,value in ref_value:
            if value != 'start':
                self.df['Reporting_PeriodStartDate']=self.df.loc[self.df[ref_col]==key]['Reporting_PeriodStartDate'] - datetime.timedelta(value)
            else:
                self.df['Reporting_PeriodStartDate']=self.df.loc[self.df[ref_col]==key]['Reporting_PeriodStartDate'].replace(day=1)
        return self.df

    def extract_features_from_description(df, 
                                      column_name,
                                      new_feature_name, 
                                      extract_words):
        # function to extract features from the column_name
        # searches column_name feature for a given list
        # ASSUMPTION: There is no NA values
        # in the description feature
        check_regex = (r'\b(?:{})\b'
                    .format('|'
                            .join(
                                map(re.escape,
                                    extract_words))))

        df[new_feature_name] = (df[column_name]
                                .strß
                                .contains(check_regex,
                                        regex=True)
                                .astype('uint8'))
        return df