import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

st.set_page_config(
    page_title="Rumah Ningrat Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

def load_css():
    try:
        with open('assets/styles.css') as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        pass

def render_sidebar():
    st.sidebar.title("Rumah Ningrat Dashboard")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Navigation",
        ["Revenue Engineering", "Organic Architecture"],
        label_visibility="collapsed",
    )

    return page

def main():
    load_css()
    page = render_sidebar()

    if page == "Revenue Engineering":
        from modules.revenue_engineering import show_revenue_engineering
        show_revenue_engineering()
    elif page == "Organic Architecture":
        from modules.organic_architecture import show_organic_architecture
        show_organic_architecture()

if __name__ == "__main__":
    main()
