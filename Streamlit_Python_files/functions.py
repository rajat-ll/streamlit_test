import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime
import streamlit as st
import time
from snowflake.connector.pandas_tools import write_pandas

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


import yaml
# Load YAML configuration file
with open("env_det.yml", 'r') as stream:
    config = yaml.safe_load(stream)

# Accessing Snowflake configuration parameters
# snowflake_config = config['snowflake']
user = config['snowflake']['sf_user']
password = config['snowflake']['sf_password']
account = config['snowflake']['sf_account']
warehouse = config['snowflake']['sf_warehouse']
database = config['snowflake']['sf_prod_database']
schema = config['snowflake']['sf_schema']


def st_read_from_snowflake(query):
    session = get_active_session()
    df = pd.DataFrame(session.sql(query).collect())
    # st.write(df.shape)
    # st.write(df.columns)
    # st.write(df.dtypes)
    return df


def st_execute_query_on_snowflake(query):
    session = get_active_session()
    try:
        session.sql(query).collect()
        # st.success(f"Requested operation completed successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"An error occurred during the requested operation: \n{e}")


def generate_update_query(row, table_name, primary_key, dataset):
    pk_value = row[primary_key]
    set_clauses = []
    
    # Data type mappings
    dtype_mappings = {
        'object': 'VARCHAR(16777216)',
        'int64': 'NUMBER(38,0)',
        'float64': 'NUMBER(38,4)',
        'datetime64[ns]': 'TIMESTAMP_LTZ(9)',
        'bool': 'BOOLEAN'
    }
    
    # Iterate over the columns of the original dataset
    for col in dataset.columns:
        # Skip the primary key column
        if col == primary_key:
            continue
        
        # Get the data type of the column from the dataset
        col_data_type = dataset[col].dtype
        
        # Get the corresponding SQL data type based on the column's data type
        sql_type = dtype_mappings.get(str(col_data_type), 'VARCHAR(16777216)')
        
        # Convert the value from the edited row to the appropriate data type
        value = row[col]
        if pd.isnull(value):
            set_value = 'NULL'
        elif col_data_type == 'bool':
            set_value = str(value).lower()  # Convert boolean value to lowercase string ('true' or 'false')
        elif col_data_type == 'datetime64[ns]':
            set_value = f"'{value}'"  # Format datetime value as a string
        elif col_data_type == 'object':
            set_value = f"'{value}'"   # Enclose string value in single quotes
        else:
            set_value = f"{value}"  # No conversion needed
        
        # Construct the update statement for the column
        if set_value == 'NULL':
            set_clause = f"{col} = {set_value}"
        else:
            set_clause = f"{col} = {set_value}::{sql_type}"
        set_clauses.append(set_clause)
    
    # Join the individual update statements to form the complete SET clause
    set_clause = ", ".join(set_clauses)
    
    # Construct the SQL update query
    update_query = f'''
    UPDATE {table_name} SET {set_clause} WHERE {primary_key} = {pk_value};
    '''
    return update_query


def generate_insert_query(row, table_name, dataset):
    # Initialize lists to store column names and values
    columns = []
    values = []
    
    # Data type mappings
    dtype_mappings = {
        'object': 'VARCHAR(16777216)',
        'int64': 'NUMBER(38,0)',
        'float64': 'NUMBER(38,4)',
        'datetime64[ns]': 'TIMESTAMP_LTZ(9)',
        'bool': 'BOOLEAN'
    }
    
    # Iterate over the columns of the original dataset
    for col in dataset.columns:
        # Get the data type of the column from the dataset
        col_data_type = dataset[col].dtype
        
        # Get the corresponding SQL data type based on the column's data type
        sql_type = dtype_mappings.get(str(col_data_type), 'VARCHAR(16777216)')
        
        # Convert the value from the row to the appropriate data type
        value = row[col]
        if pd.isnull(value):
            value = 'NULL'
        elif col_data_type == 'bool':
            value = str(value).lower()  # Convert boolean value to lowercase string ('true' or 'false')
        elif col_data_type == 'datetime64[ns]':
            value = f"'{value}'"  # Format datetime value as a string
        elif col_data_type == 'object':
            value = f"'{value}'"   # Enclose string value in single quotes
        else:
            value = f"{value}"  # No conversion needed
        
    
        # Add column name and value to respective lists
        columns.append(f'{col}')
        if value == 'NULL':
            values.append(f"{value}")
        else:
            values.append(f"{value}::{sql_type}")
    
    # Join column names and values to form the insert statement
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    # Construct the SQL insert query
    insert_query = f'''
    INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});
    '''
    return insert_query


def apply_filters(df, filters):
    filtered_df = df.copy()
    for column, value in filters.items():
        filtered_df = filtered_df[filtered_df[column] == value]
    return filtered_df