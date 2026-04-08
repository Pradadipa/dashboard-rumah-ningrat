import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

# Pastikan set_page_config ada di baris paling atas
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
        ["Paid Ads Campaign Performance", "Social Media Performance"],
        label_visibility="collapsed",
    )

    return page

def main():
    load_css()
    
    # --- 1. LOAD KONFIGURASI YAML ---
    config = st.secrets

    # --- 2. INISIALISASI AUTHENTICATOR ---
    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
    )

    # --- 3. RENDER FORM LOGIN ---
    try:
        authenticator.login()
    except Exception as e:
        st.error(e)

    # --- 4. LOGIKA AKSES HALAMAN ---
    if st.session_state["authentication_status"]:
        # Tampilkan tombol logout di sidebar jika berhasil login
        authenticator.logout('Logout', 'sidebar')
        
        # Render navigasi utama HANYA jika sudah login
        page = render_sidebar()

        if page == "Paid Ads Campaign Performance":
            from modules.revenue_engineering import show_revenue_engineering
            show_revenue_engineering()
        elif page == "Social Media Performance":
            from modules.organic_architecture import show_organic_architecture
            show_organic_architecture()
            
    elif st.session_state["authentication_status"] is False:
        st.error('Username atau password salah')
        
    elif st.session_state["authentication_status"] is None:
        st.warning('Silakan masukkan username dan password untuk mengakses dashboard')

if __name__ == "__main__":
    main()