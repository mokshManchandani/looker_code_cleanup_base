# Code Clean UP api base

this api currently has one route of generate_report which would do the following
- download data from the git repository (this is set in a .env file) hence is static for now
- parse the downloaded view files from github to generate the required values from a view file(s) and store the data as a parquet file
- this api also downloads data from the system activity history explore to get the fields that are queried and the respective query ids similarly it is saved as a parquet file
- all the parquet files are then pushed to a bigquery dataset the dataset id is saved in .env file


the part of code that would push the data to bigquery is commented out

things to make sure while trying to run this script
- having a valid service account key for your gcp account and point it to the GOOGLE_APPLICATION_CREDENTIALS key in the .env file
- have a looker.ini file ready with the a section called Looker with the base_url, client_id and client_secret for connecting to the looker instance also make sure your id and secret combination have api access enabled
- for local development hardcode the REPO_LINK key in .env file without this the github service wont work
  
to install dependencies in the cloned project run the command `pip install -r requirements.txt`
to run local development move to the cloned folder and run the command `python main.py` it should run a development server on port `3000`
