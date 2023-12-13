# this module takes care of downloading the data from the system activity explore on our instance
import looker_sdk
from looker_sdk.sdk.api40 import  models
import pandas as pd
import json
import os


class SysActivityDownloader:
    def __init__(self,model_name):
        self.file_save_path = os.path.join("tmp","sys_activity_history.csv")
        self.csv_save_path = os.path.join('tmp','sys_data.csv')
        self.parquet_save_path = os.path.join("bq_upload_data","sys_activity_history.parquet")
        self.dashboards = []
        self.sdk = looker_sdk.init40()
        self.model_name = model_name


    def fetch_data(self):
        """
        this function queries the system activity explore for query level values
        """
        resp = self.sdk.run_inline_query(
            result_format='csv',
            body=models.WriteQuery(
                model = 'system__activity',
                view = 'history',
                fields = [
                    "query.formatted_fields",
                    "query.created_date",
                    "query.id",
                    "query.count",                                                                                  
                ],
                filters={
                    "query.model":self.model_name,
                    "query.formatted_fields":"-NULL"
                }
            )
        )
        with open(self.csv_save_path,'w') as f:
            f.write(resp)
    def create_df(self):
        self.fetch_data()
        df = pd.read_csv(self.csv_save_path).rename(columns={
            "Query Fields Used":"used_fields",
            "Query Created Date":"created_data",
            "Query ID":"id",
            "Query Count":"count"
        })
        df['used_fields'] = df['used_fields'].apply(lambda obj: json.loads(obj))
        df = df.explode("used_fields")
        df.to_csv(self.file_save_path,index=False)
        # df.to_parquet(self.parquet_save_path,index=False)
        
        
        
        

        
