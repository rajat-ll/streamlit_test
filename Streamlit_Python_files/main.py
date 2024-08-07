import streamlit as st
from login_page import login
from data_edit_ui import edit_ui
from functions import get_snowflake_session, load_and_display_csv

def main():
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'login'

    if st.session_state.current_page == 'login':
        login()
    elif st.session_state.current_page == 'editui':
        edit_ui()
    elif st.session_state.current_page == 'load_files':
        load_files()

def load_files():
    st.title("Load Files from Stage")

    # Snowflake connection
    conn = get_snowflake_session()

    # List all files in the stage
    list_files_query = "LIST @DATA_EDIT_UI_RAJAT"
    files_df = pd.read_sql(list_files_query, conn)

    for index, row in files_df.iterrows():
        file_path = row['name']
        file_name = file_path.split('/')[-1].split('.')[0]

        st.write(f"File: {file_name}")

        if file_name.endswith('.csv'):
            load_and_display_csv(conn, file_name, file_path)

    conn.close()

if __name__ == "__main__":
    main()
