APP_NAME = "Rumah Ningrat Dashboard"
VERSION = "1.0.0"

# Color Scheme (Dark Mode)
COLORS = {
    'background': '#0E1117',
    'secondary_bg': '#1E1E1E',
    'text': '#FAFAFA',
    'primary': '#00D9FF',  # Neon Blue
    'success': '#00FF88',  # Neon Green
    'warning': '#FFB800',  # Neon Orange
    'danger': '#FF0055',   # Neon Red
    'purple': '#B026FF',   # Neon Purple
}

# Funnel Stages
FUNNEL_STAGES = {
    'TOF': {'name': 'Top of Funnel', 'color': COLORS['primary'], 'icon': '🔵'},
    'MOF': {'name': 'Middle of Funnel', 'color': COLORS['purple'], 'icon': '🟣'},
    'BOF': {'name': 'Bottom of Funnel', 'color': COLORS['warning'], 'icon': '🟠'},
    'RET': {'name': 'Retention', 'color': COLORS['success'], 'icon': '🟢'},
}

# Target Metrics
TARGETS = {
    'MER': 3.0,
    'LTV_CAC': 3.0,
    'CVR': 2.5,
    'ENGAGEMENT_RATE': 5.0,
}

# API Configuration (will use later)
OPENAI_MODEL = "gpt-3.5-turbo"