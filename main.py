import shutil
import os
from pathlib import Path


from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv


from scripts import github_downloader,lkml_parser,download_sys_activity,bigquery_service
app = Flask(__name__)
CORS(app)
load_dotenv('.env')

@app.route("/generate_report",methods=["GET"])
def clean_up():
    # this path stores the data for storing parquet files for bq
    upload_path =  Path('bq_upload_data')
    upload_path.mkdir()
    
    # download the data from the provided repo link
    g_downloader = github_downloader.GitDownloader(repo_link=os.getenv('REPO_LINK'))
    g_downloader.download_content()

    #parse the view data and also extract the model name
    l_parser = lkml_parser.LKMLParser(g_downloader.view_file_glob,model_file_glob=g_downloader.model_file_glob)
    l_parser.get_model_name()
    l_parser.create_df()

    # get the required system activity data from the looker
    s_downloader = download_sys_activity.SysActivityDownloader(l_parser.model_name)
    s_downloader.create_df()

    # send data to bq
    # uncomment the data from this for running bigquery service and sending data
    # in the mapping
    # table_name -> file_name
    # name_mapping = {
    #     "parsed_view_test":l_parser.parquet_save_path,
    #     "sys_activity_history_test":s_downloader.parquet_save_path
    # }
    # bq_service = bigquery_service.BigqueryService(name_mapping=name_mapping)
    # bq_service.create_tables()

    # remove all files that are set
    shutil.rmtree("tmp")
    shutil.rmtree(upload_path)
    return {"message":"done"}



if __name__ == "__main__":
    # local dev happens on port 3000
    app.run(debug=True, port=3000)
