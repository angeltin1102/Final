from __future__ import annotations

import logging
import os
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

# Check if virtualenv is installed
if not is_venv_installed():
    log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")
else:
    @dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=["big_data"])
    def homework2_data_pipeline_demo():
        """
        ### TaskFlow API example using virtualenv
        This is a simple data pipeline example which demonstrates the use of the TaskFlow API using three simple tasks for Extract, Transform, and Load.
        """

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["funcsigs", "pandas"],  # Updated requirements
        )
        def data_acquisition():
            """
            #### Data Acquisition
            This task will download data from an internet data source and save data to a specific location that is "shareable" with the other tasks.
            """
            import pandas as pd
            import os

            # Define data acquisition parameters
            data_id = ".csv"
            data_path = "/storage/acquire/" + data_id

            # Create directories if they don't exist
            os.makedirs(os.path.dirname(data_path), exist_ok=True)

            '''
            # Simulate data acquisition
            data = {
                "StateName": ["CA"],
            }
            
            df = pd.DataFrame(data)
            '''
            df = pd.read_csv(data_path)


            # Save data as CSV
            df.to_csv(data_path, index=False)



            return {"status": "success", "data_path": data_path}
        @task()
        def data_split(data_package: dict):
            ##Split .cvs into diff states then pass through cleanse
            """
            #### Data Split
            This task splits the CSV file into separate files for each US state.
            """
            import pandas as pd
            import os

            # Retrieve CSV file path from the data package
            csv_file_path = data_package.get("data_path")

            # Read CSV file
            try:
                df = pd.read_csv(csv_file_path)
            except Exception as e:
                return {"status": "failed", "error": str(e)}  # Return failure status if reading CSV fails

            # Get unique US states from the dataframe
            us_states = df['StateName'].unique()

            # Create a directory to store the split files
            split_folder = "/storage/split"
            os.makedirs(split_folder, exist_ok=True)

            # Iterate over each state and write a separate CSV file
            split_files = []
            for state in us_states:
                state_df = df[df['StateName'] == state]
                state_file_path = os.path.join(split_folder, f"{state}.csv")
                state_df.to_csv(state_file_path, index=False)
                split_files.append(state_file_path)

            return {"status": "success", "split_files": split_files}

        @task()
        def data_cleanse(data_package: dict):
            """
            #### Data Cleanse
            This task cleanses each US state CSV file by removing any column headers that don't start with "2023".
            """
            import pandas as pd
            import os

            # Retrieve split file paths from the data package
            split_files = data_package.get("split_files")

            # Create a folder to store the cleansed files
            cleanse_folder = "/storage/cleanse"
            os.makedirs(cleanse_folder, exist_ok=True)

            # Iterate through each split file
            for split_file in split_files:
                try:
                    # Read the split file
                    df = pd.read_csv(split_file)

                    # Filter columns that start with "2023"
                    columns_to_keep = [col for col in df.columns if col.startswith("2023") or col.startswith("2024")]
                    df = df[columns_to_keep]

                    # Define cleanse file path
                    cleanse_file_path = os.path.join(cleanse_folder, os.path.basename(split_file))

                    # Save cleansed data as CSV
                    df.to_csv(cleanse_file_path, index=False)

                except Exception as e:
                    return {"status": "failed", "error": str(e)}  # Return failure status if any error occurs

            return {"status": "success", "cleanse_folder": cleanse_folder}
            """
            #### Data Cleanse
            This task cleans the data given a path to the recently acquired data.
            
            import pandas as pd
            import os

            # Retrieve CSV file path from the data package
            csv_file_path = data_package.get("data_path")  

            # Read CSV file
            try:
                df = pd.read_csv(csv_file_path)
            except Exception as e:
                return {"status": "failed", "error": str(e)}  # Return failure status if reading CSV fails

            # Filter data
            df = df[df['StateName'].str.contains('CA', na=False)]  

            # Define cleanse folder and file path
            cleanse_folder = "/storage/cleanse"
            os.makedirs(cleanse_folder, exist_ok=True)
            cleanse_file_path = os.path.join(cleanse_folder, os.path.basename(csv_file_path))

            # Save cleansed data as CSV
            try:
                df.to_csv(cleanse_file_path, index=False)
            except Exception as e:
                return {"status": "failed", "error": str(e)}  # Return failure status if writing CSV fails

            return {"status": "success", "cleanse_path": cleanse_file_path}
            """


        @task()
        def data_analysis(data_package: dict):
            """
            #### Analysis
            This task analyzes the data given a list of file paths for each US state CSV file.
            """
            import pandas as pd
            import os

            # Check if virtualenv is installed
            from airflow.operators.python import is_venv_installed
            if not is_venv_installed():
                log.warning("The data_analysis task requires scikit-learn, which is not installed. Installing now.")
                try:
                    import sys
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn"])
                except Exception as e:
                    return {"status": "failed", "error": str(e)}

            # Now that scikit-learn is installed, import it
            try:
                from sklearn.linear_model import LinearRegression
                from sklearn.model_selection import train_test_split
                from sklearn.metrics import mean_squared_error
            except ImportError as e:
                return {"status": "failed", "error": str(e)}

             # Retrieve list of file paths for each US state CSV file from the data package
            state_files = data_package.get("split_files")

            # Create a folder to store the analysis results
            analysis_folder = "/storage/analysis"
            os.makedirs(analysis_folder, exist_ok=True)

            # Initialize a dictionary to store analysis results for each state
            analysis_results = {}

            # Loop through each state file
            for state_file in state_files:
                try:
                    # Read the state CSV file
                    df = pd.read_csv(state_file)

                    # Prepare features (numerical columns) and labels (rent prices)
                    features = df.drop(columns=['Rent'])
                    labels = df['Rent']

                    # Split data into training and testing sets
                    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)

                    # Train a linear regression model
                    model = LinearRegression()
                    model.fit(X_train, y_train)

                    # Predict future rent prices
                    future_rent_prices = model.predict(features)

                    # Calculate mean squared error
                    mse = mean_squared_error(labels, future_rent_prices)

                    # Store analysis results for the state
                    state_name = os.path.splitext(os.path.basename(state_file))[0]  # Extract state name from file name
                    analysis_results[state_name] = {"future_rent_prices": future_rent_prices, "mse": mse}

                except Exception as e:
                    return {"status": "failed", "error": str(e)}  # Return failure status if any error occurs

            # Save analysis results as files
            for state, result in analysis_results.items():
                result_file_path = os.path.join(analysis_folder, f"{state}_analysis.txt")
                with open(result_file_path, "w") as f:
                    f.write(f"Future rent prices: {result['future_rent_prices']}\n")
                    f.write(f"Mean Squared Error: {result['mse']}\n")

            return {"status": "success", "analysis_folder": analysis_folder}
            """
            #### Analysis
            This task analyzes the data given a path to the recently cleansed data.
            
            import os
            
            # Define analysis data path
            data_id = "1234"
            data_path = "/storage/analysis/" + data_id

            # Simulate analysis by creating a placeholder file
            with open(data_path, "w") as file:
                file.write("Placeholder analysis result")

            return {"status": "success", "data_path": data_path}
            """

        @task()
        def visualize(data_package: dict):
            """
            #### Visualize
            This task performs visualization on the data given path to
            
            # Placeholder visualization task
            log.info("Visualization task completed.")
            """

        # Define data flow
        data_a = data_acquisition()
        data_b = data_split(data_a)
        data_c = data_cleanse(data_b)
        data_d = data_analysis(data_c)

        visualize(data_d)

    python_demo_dag = homework2_data_pipeline_demo()
