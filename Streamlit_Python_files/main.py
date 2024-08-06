import streamlit as st
from login_page import login
from data_edit_ui import edit_ui

def main():
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'login'

    if st.session_state.current_page == 'login':
        login()
    elif st.session_state.current_page == 'editui':
        edit_ui()

if __name__ == "__main__":
    main()
