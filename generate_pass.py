import streamlit_authenticator as stauth

# Cara baru untuk Streamlit-Authenticator versi terbaru
password_asli = '*rumahningrat112#'  # Ganti dengan password asli yang ingin di-hash

# Gunakan method .hash() secara langsung untuk satu password
hashed_password = stauth.Hasher.hash(password_asli)

print(hashed_password)