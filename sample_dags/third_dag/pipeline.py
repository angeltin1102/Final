from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")
else:
    @dag(schedule=None, 
         start_date=datetime(2021, 1, 1), 
         catchup=False, 
         tags=["big_data"])

    def homework2_data_pipeline_demo():
        """
        ### TaskFlow API example using virtualenv

        This is a simple data pipeline example which demonstrates the use of the TaskFlow API using three simple tasks for Extract, Transform, and Load.
        """

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["funcsigs"],
        )
        def data_aquisition():
            """
            #### Data Acquisition
            This task will download data from an internet data source and save data to a specific location that is "shareable" with the other tasks.
            """

            ## import modules/libraries
            import pandas as pd
            import json
            import os

            ## perform data acquisition
            data_id = "Metro_zori_uc_sfrcondomfr_sm_month.csv"
            ## save data
            data_path = "/storage/acquire/" + data_id

            # Check if the CSV file exists
            if not os.path.isfile(data_path):
                return {"status": "error", "message": "CSV file not found."}
            
            df = pd.read_csv(data_path)

            storage_folder = "storage/acquire"
            if not os.path.exists(storage_folder):
                os.makedirs(storage_folder)

            json_file_path = os.path.join(storage_folder, os.path.splitext(data_id)[0] + ".json")
            df.to_json(json_file_path, orient="records")

            ## return status
            return {"status": "success", "data_path": json_file_path}

        @task()
        def data_cleanse(data_package: dict):
            """
            #### Data Cleanse
            This tasks cleans the data given a path to the recently acquired data.
            """

            ### import modules/libraries
            import json

            # Load JSON data
            json_file_path = data_package.get("data_path", "")
            if not os.path.isfile(json_file_path):
                return {"status": "error", "message": "JSON file not found."}

            # Read JSON into DataFrame
            df = pd.read_json(json_file_path)

            # Filter rows where RegionName contains "CA"
            df = df[df['RegionName'].str.contains('CA', na=False)]

            # Save cleaned data as JSON
            cleanse_folder = "storage/cleanse"
            if not os.path.exists(cleanse_folder):
                os.makedirs(cleanse_folder)
            
            cleanse_file_path = os.path.join(cleanse_folder, os.path.basename(json_file_path))
            df.to_json(cleanse_file_path, orient="records")

            # Return status
            return {"status": "success", "cleanse_path": cleanse_file_path}

            '''
            ## perform data cleansing
            data_id = "Metro_zori_uc_sfrcondomfr_sm_month.csv"
            

            ## save data
            data_path = "/storage/cleanse/" + data_id

            ## return status
            return {"status" : "success", "data_path" : data_path}
            '''

        @task()
        def data_analysis(data_package: dict):
            """
            #### Analysis
            This task analyzes the data given a path to the recently cleansed data.
            """
            ### import modules/libraries
            import json

            ## perform data cleansing
            data_id = "1234"

            ## save data
            data_path = "/storage/analysis/" + data_id

            ## return status
            return {"status" : "success", "data_path" : data_path}


        @task()
        def visualize(data_package: dict):
            """
            #### Visualize
            This task performs visualization on the data given path to 

            """

            return 
        ## data flow

        data_a = data_aquisition()
        data_b = data_cleanse(data_a)
        data_c = data_analysis(data_b)

        visualize(data_c)

    python_demo_dag = homework2_data_pipeline_demo()