# this module works for parsing the view files and model file parsed from github
# NOTE: For now just a single model file is being considered in the script

from lkml import load
import pandas as pd
from copy import deepcopy
import os

class LKMLParser:
    def __init__(self,view_file_glob,model_file_glob):
        self.view_file_glob = view_file_glob
        self.view_level_values = ['dimensions','measures','name','sql_table_name','derived_table']
        self.fields = ['dimensions','measures']
        self.content = []
        self.model_name = ''
        self.model_file_glob = model_file_glob
        self.all_content = []
        self.save_path = os.path.join("tmp","parsed_view_data.csv")
        self.parquet_save_path = os.path.join("bq_upload_data","parsed_view_data.parquet")
    
    def get_model_name(self):
        """
        utility function to read model name
        """
        for path in self.model_file_glob:
            self.model_name = path.stem.split('.model')[0]
    
    def read_view_files(self):
        """
        Utility function to read view file lookml code
        """
        content = []
        for path in self.view_file_glob:
            with open(path, 'r') as f:
                content.append(load(f)['views'][0])
        self.content = content
    
    
    def sanitize_content(self):
        """
        from all the content just keep the field level values
        this is for simplicity sake
        """
        sanitized_content = []
        for content in self.content:
            new_content = deepcopy(content)
            for key in content.keys():
                if key not in self.view_level_values: del new_content[key]
            sanitized_content.append(new_content)

        self.content = sanitized_content
    
    def __create_structure(self,content_obj):
        """
        This function puts the parsed code into a format which then can be converted into a tabular format
        """
        view_name = content_obj.get('name','NO_NAME_DEFINED')
        for key, value in content_obj.items():
            if key in self.fields:
                for value_obj in value:
                    # structure for a row in the view level table
                    data = {
                        'view_name': view_name,
                        'model_name': self.model_name,
                        'field_name': value_obj.get('name','no_name_provided'),
                        'field_type': key,
                        'description':value_obj.get('description','no_description_provided'),
                        'used_fields':f"{view_name}.{value_obj['name']}"
                    }
                    self.all_content.append(data)

    def create_df(self):
        """
        Loop through each parsed block in the view file and put them in the respective
        metadata table and view level data
        """
        self.read_view_files()
        self.sanitize_content()
        for content in self.content:
            self.__create_structure(content)
        
        df = pd.DataFrame(self.all_content)
        df.to_csv(self.save_path,index=False)
        # df.to_parquet(self.parquet_save_path,index=False)
