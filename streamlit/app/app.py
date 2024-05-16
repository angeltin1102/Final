import streamlit as st
import pandas as pd
import os
import numpy as np

def loaddata(filepath):
    """
    Load data from a CSV file.
    """
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        return df
    else:
        st.error("File not found.")
        return None

def main():
    # Set page title
    st.title("Line Graph from CSV")

    # Load data
    data_id = "CA.csv"
    file_path = "/storage/cleanse/" + data_id
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df = loaddata(file_path)

    # Display data
    if df is not None:
        st.write("### Data from CSV file")
        st.write(df)

        # Plot line graph
        st.write("### Line Graph")
        st.line_chart(df)

        # Display summary statistics
        st.write("### Summary Statistics")
        st.write(df.describe())

        # # Display missing values
        # st.write("### Missing Values")
        # missing_values = df.isnull().sum()
        # st.write(missing_values)

        # Display correlation matrix
        st.write("### Correlation Matrix")
        st.write(df.corr())

        # # Display scatter plot
        # st.write("### Scatter Plot")
        # st.write("Select columns to plot:")
        # x_column = st.selectbox("X-axis", df.columns)
        # y_column = st.selectbox("Y-axis", df.columns)
        # st.write(f"Selected columns: {x_column}, {y_column}")
        # st.write(df.plot.scatter(x=x_column, y=y_column))

if __name__ == "__main__":
    main()