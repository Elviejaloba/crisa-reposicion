import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import database as db
from io import BytesIO
from contextlib import contextmanager

SPINNER_CSS = """
<style>
@keyframes scissors-cut {
    0%, 100% { transform: rotate(0deg); }
    25% { transform: rotate(-20deg); }
    50% { transform: rotate(0deg); }
    75% { transform: rotate(-20deg); }
}
@keyframes scissors-move {
    0%, 100% { transform: translateX(0px); }
    50% { transform: translateX(10px); }
}
.tijera-spinner {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    gap: 1rem;
}
.tijera-icon {
    font-size: 3rem;
    animation: scissors-cut 0.6s ease-in-out infinite, scissors-move 1.2s ease-in-out infinite;
}
.tijera-text {
    font-size: 1rem;
    color: #6b7280;
    animation: pulse 1.5s ease-in-out infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}
</style>
"""

def show_loading_spinner(message="Cargando datos..."):
    return st.markdown(f"""
        {SPINNER_CSS}
        <div class="tijera-spinner">
            <div class="tijera-icon">✂️</div>
            <div class="tijera-text">{message}</div>
        </div>
    """, unsafe_allow_html=True)

st.set_page_config(
    page_title="Reposición de Sucursales",
    page_icon="static/logo.png",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Procesar acciones del tour ANTES de renderizar cualquier contenido
if "tour_action" in st.query_params:
    action = st.query_params["tour_action"]
    st.query_params.clear()
    
    # Inicializar si no existe
    if "tour_step" not in st.session_state:
        st.session_state.tour_step = 0
    if "show_tour" not in st.session_state:
        st.session_state.show_tour = True
    
    if action == "next":
        st.session_state.tour_step = min(st.session_state.tour_step + 1, 7)
        st.rerun()
    elif action == "prev":
        st.session_state.tour_step = max(st.session_state.tour_step - 1, 0)
        st.rerun()
    elif action in ["finish", "close"]:
        st.session_state.show_tour = False
        st.session_state.tour_step = 0
        st.rerun()

PASSWORD = "2809"

def check_password():
    """Verifica la contraseña de acceso"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown("""
        <style>
            [data-testid="stAppViewContainer"] {
                background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
            }
            [data-testid="stHeader"] { background: transparent; }
            [data-testid="stSidebar"] { display: none; }
            #MainMenu, footer { visibility: hidden; }
            
            /* Centrar columna principal */
            [data-testid="stMainBlockContainer"] {
                max-width: 400px !important;
                margin: 0 auto !important;
                padding-top: 10vh !important;
            }
            
            .login-card {
                background: rgba(255, 255, 255, 0.05);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 20px;
                padding: 40px 35px;
                text-align: center;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
            }
            .login-title {
                color: #ffffff;
                font-size: 22px;
                font-weight: 600;
                margin: 20px 0 8px 0;
                text-align: center;
            }
            .login-subtitle {
                color: #94a3b8;
                font-size: 14px;
                margin: 0 0 25px 0;
                text-align: center;
            }
            
            /* Input */
            [data-testid="stTextInput"] > div > div {
                background: rgba(255, 255, 255, 0.08) !important;
                border: 1px solid rgba(255, 255, 255, 0.15) !important;
                border-radius: 10px !important;
            }
            [data-testid="stTextInput"] input {
                color: white !important;
                text-align: center !important;
                font-size: 18px !important;
                letter-spacing: 8px !important;
            }
            [data-testid="stTextInput"] input::placeholder {
                color: #64748b !important;
            }
            
            /* Button */
            .stButton > button {
                background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
                border: none !important;
                border-radius: 10px !important;
                padding: 12px 30px !important;
                font-weight: 600 !important;
                margin-top: 10px !important;
            }
            .stButton > button:hover {
                transform: translateY(-2px) !important;
                box-shadow: 0 10px 25px rgba(59, 130, 246, 0.4) !important;
            }
            /* Error centrado */
            [data-testid="stAlert"] {
                text-align: center;
            }
        </style>
        <script>
            // Desactivar autocompletado
            document.addEventListener('DOMContentLoaded', function() {
                setTimeout(function() {
                    var inputs = document.querySelectorAll('input[type="password"]');
                    inputs.forEach(function(input) {
                        input.setAttribute('autocomplete', 'off');
                        input.setAttribute('data-lpignore', 'true');
                        input.setAttribute('data-form-type', 'other');
                    });
                }, 500);
            });
        </script>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image("static/logo.png", width="stretch")
        
        st.markdown("""
            <p class="login-title">Reposición de Sucursales</p>
            <p class="login-subtitle">Ingrese la clave de acceso</p>
        """, unsafe_allow_html=True)
        
        password_input = st.text_input("Clave", type="password", label_visibility="collapsed", placeholder="••••")
        
        if st.button("Ingresar", use_container_width=True, type="primary"):
            if password_input == PASSWORD:
                st.session_state.authenticated = True
                # Activar tour guiado al iniciar sesión
                st.session_state.show_tour = True
                st.session_state.tour_step = 0
                st.rerun()
            else:
                st.error("Clave incorrecta")
        
        return False
    return True

if not check_password():
    st.stop()

if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

def get_theme_styles():
    spinner_override = """
            /* Custom Tijera Spinner */
            div[data-testid="stSpinner"] > div {
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 0.5rem;
            }
            div[data-testid="stSpinner"] > div::before {
                content: "✂️";
                font-size: 2.5rem;
                animation: scissors-cut 0.6s ease-in-out infinite, scissors-move 1.2s ease-in-out infinite;
            }
            @keyframes scissors-cut {
                0%, 100% { transform: rotate(0deg); }
                25% { transform: rotate(-20deg); }
                50% { transform: rotate(0deg); }
                75% { transform: rotate(-20deg); }
            }
            @keyframes scissors-move {
                0%, 100% { transform: translateX(0px); }
                50% { transform: translateX(8px); }
            }
    """
    if st.session_state.dark_mode:
        return "<style>" + spinner_override + """
            .stApp { background-color: #1a1a2e; }
            .main-header { font-size: 2rem; font-weight: 700; color: #e2e8f0; margin-bottom: 1rem; }
            .metric-card { background: linear-gradient(135deg, #4c1d95 0%, #7c3aed 100%); padding: 1rem; border-radius: 10px; color: white; }
            .stMetric { background-color: #2d2d44; padding: 1rem; border-radius: 10px; border: 1px solid #4a4a6a; }
            .stMetric label { font-size: 0.85rem !important; color: #a0aec0; }
            div[data-testid="stMetricValue"] { font-size: 1.5rem; color: #e2e8f0; }
            div[data-testid="stMetricDelta"] { color: #a0aec0; }
            .block-container { padding-top: 2rem; padding-bottom: 2rem; }
            .alert-quiebre { background-color: #4a1a1a; border-left: 4px solid #ef4444; padding: 0.5rem 1rem; border-radius: 4px; color: #fca5a5; }
            .alert-normal { background-color: #1a4a2e; border-left: 4px solid #22c55e; padding: 0.5rem 1rem; border-radius: 4px; color: #86efac; }
            /* DataFrame - Sin estilos personalizados para no interferir con el canvas */
            .stDataFrame { border-radius: 10px; overflow: hidden; }
            
            /* Tooltips - Glide Data Grid y otros */
            div[data-testid="stExpander"] { background-color: #2d2d44 !important; border: 1px solid #4a4a6a !important; border-radius: 8px !important; }
            div[data-testid="stExpander"] summary { color: #ffffff !important; background-color: #374151 !important; }
            div[data-testid="stExpander"] summary * { color: #ffffff !important; }
            div[data-testid="stExpander"] summary p { color: #ffffff !important; }
            div[data-testid="stExpander"] summary span { color: #ffffff !important; }
            div[data-testid="stExpander"] summary div { color: #ffffff !important; }
            div[data-testid="stExpander"] details summary span { color: #ffffff !important; }
            div[data-testid="stExpander"] [data-testid="stExpanderDetails"] { background-color: #2d2d44 !important; }
            .streamlit-expanderHeader { color: #ffffff !important; background-color: #374151 !important; }
            .streamlit-expanderHeader * { color: #ffffff !important; }
            details[data-testid="stExpander"] > summary { color: #ffffff !important; background-color: #374151 !important; }
            details[data-testid="stExpander"] > summary * { color: #ffffff !important; }
            [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p { color: #ffffff !important; }
            .st-emotion-cache-p5msec { color: #ffffff !important; background-color: #374151 !important; }
            .st-emotion-cache-p5msec * { color: #ffffff !important; }
            section[data-testid="stSidebar"] { background-color: #16213e; }
            .stSelectbox label, .stMultiSelect label, .stTextInput label { color: #e2e8f0 !important; }
            .stTabs [data-baseweb="tab-list"] { 
                background: #1e293b !important; 
                border-radius: 10px; 
                padding: 6px; 
                gap: 8px;
                border: 1px solid #334155;
                box-shadow: inset 0 1px 2px rgba(0,0,0,0.2);
            }
            .stTabs [data-baseweb="tab"] { 
                color: #94a3b8 !important; 
                font-weight: 500;
                font-size: 0.95rem;
                padding: 10px 24px;
                border-radius: 8px;
                transition: all 0.2s ease;
                background: transparent !important;
            }
            .stTabs [data-baseweb="tab"]:hover { 
                background-color: #334155 !important; 
                color: #e2e8f0 !important;
            }
            .stTabs [aria-selected="true"] { 
                color: #ffffff !important; 
                background: #475569 !important;
                box-shadow: 0 1px 3px rgba(0,0,0,0.3);
                font-weight: 600;
            }
            .stTabs [data-baseweb="tab-highlight"] { display: none; }
            .stTabs [data-baseweb="tab-border"] { display: none; }
            
            /* Selectbox y Multiselect en modo oscuro */
            .stSelectbox > div > div, .stMultiSelect > div > div {
                background-color: #374151 !important;
                color: #e2e8f0 !important;
                border: 1px solid #4a5568 !important;
                border-radius: 8px !important;
            }
            .stMultiSelect [data-baseweb="tag"] {
                background-color: #6366f1 !important;
                color: #ffffff !important;
            }
            .stTextInput > div > div > input {
                background-color: #374151 !important;
                color: #e2e8f0 !important;
                border: 1px solid #4a5568 !important;
            }
            [data-baseweb="popover"], [data-baseweb="menu"] {
                background-color: #374151 !important;
            }
            [data-baseweb="menu"] li {
                background-color: #374151 !important;
                color: #e2e8f0 !important;
            }
            [data-baseweb="menu"] li:hover {
                background-color: #4a5568 !important;
            }
            
            h1, h2, h3, h4, h5, h6, p, span, label { color: #e2e8f0; }
            .stMarkdown { color: #e2e8f0; }
            .theme-toggle { position: fixed; top: 10px; right: 20px; z-index: 1000; }
            .familia-btn { display: inline-block; padding: 0.3rem 0.6rem; margin: 0.1rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
            .positive-need { background-color: #4a1a1a; color: #fca5a5; }
            .negative-need { background-color: #1a4a2e; color: #86efac; }
            @media (max-width: 768px) { 
                .main-header { font-size: 1.3rem !important; white-space: normal !important; }
                div[data-testid="stMetricValue"] { font-size: 1.1rem; }
                .stMetric { padding: 0.5rem; }
                .block-container { padding: 1rem 0.5rem; }
                .stTabs [data-baseweb="tab"] { 
                    padding: 8px 16px !important;
                    font-size: 0.85rem !important;
                }
                [data-testid="column"] { min-width: 0 !important; }
            }
            @media (max-width: 480px) { 
                .main-header { font-size: 1rem !important; letter-spacing: 0 !important; }
                div[data-testid="stMetricValue"] { font-size: 0.95rem; }
                .stTabs [data-baseweb="tab"] { 
                    padding: 6px 12px !important;
                    font-size: 0.8rem !important;
                }
                .stTabs [data-baseweb="tab-list"] { 
                    padding: 4px !important;
                    gap: 4px !important;
                }
            }
            #MainMenu { visibility: hidden; }
            header[data-testid="stHeader"] button { display: none; }
        </style>
        """
    else:
        return "<style>" + spinner_override + """
            .stApp { background-color: #ffffff !important; }
            .main-header { font-size: 2rem; font-weight: 700; color: #1f2937; margin-bottom: 1rem; }
            .metric-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 10px; color: white; }
            .stMetric { background-color: #f8fafc; padding: 1rem; border-radius: 10px; border: 1px solid #e2e8f0; }
            .stMetric label { font-size: 0.85rem !important; color: #4a5568 !important; }
            .block-container { padding-top: 2rem; padding-bottom: 2rem; }
            div[data-testid="stMetricValue"] { font-size: 1.5rem; color: #1f2937 !important; }
            .alert-quiebre { background-color: #fee2e2; border-left: 4px solid #ef4444; padding: 0.5rem 1rem; border-radius: 4px; }
            .alert-normal { background-color: #dcfce7; border-left: 4px solid #22c55e; padding: 0.5rem 1rem; border-radius: 4px; }
            .stDataFrame { border-radius: 10px; overflow: hidden; }
            @media (max-width: 768px) { 
                .main-header { font-size: 1.3rem !important; white-space: normal !important; }
                div[data-testid="stMetricValue"] { font-size: 1.1rem; }
                .stMetric { padding: 0.5rem; }
                .block-container { padding: 1rem 0.5rem; }
                .stTabs [data-baseweb="tab"] { 
                    padding: 8px 16px !important;
                    font-size: 0.85rem !important;
                }
                [data-testid="column"] { min-width: 0 !important; }
            }
            @media (max-width: 480px) { 
                .main-header { font-size: 1rem !important; letter-spacing: 0 !important; }
                div[data-testid="stMetricValue"] { font-size: 0.95rem; }
                .stTabs [data-baseweb="tab"] { 
                    padding: 6px 12px !important;
                    font-size: 0.8rem !important;
                }
                .stTabs [data-baseweb="tab-list"] { 
                    padding: 4px !important;
                    gap: 4px !important;
                }
            }
            #MainMenu { visibility: hidden; }
            header[data-testid="stHeader"] button { display: none; }
            section[data-testid="stSidebar"] { background-color: #f8fafc !important; }
            
            /* Tooltips con texto blanco */
            div[data-baseweb="tooltip"] { background-color: #374151 !important; }
            div[data-baseweb="tooltip"] * { color: #ffffff !important; }
            .familia-btn { display: inline-block; padding: 0.3rem 0.6rem; margin: 0.1rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
            .positive-need { background-color: #fee2e2; color: #dc2626; }
            .negative-need { background-color: #dcfce7; color: #16a34a; }
            h1, h2, h3, h4, h5, h6 { color: #1f2937 !important; }
            p, span, label, .stMarkdown { color: #374151 !important; }
            .stTabs [data-baseweb="tab-list"] { 
                background: #f8fafc !important; 
                border-radius: 10px; 
                padding: 6px; 
                gap: 8px;
                border: 1px solid #e2e8f0;
                box-shadow: inset 0 1px 2px rgba(0,0,0,0.05);
            }
            .stTabs [data-baseweb="tab"] { 
                color: #64748b !important; 
                font-weight: 500;
                font-size: 0.95rem;
                padding: 10px 24px;
                border-radius: 8px;
                transition: all 0.2s ease;
                background: transparent !important;
            }
            .stTabs [data-baseweb="tab"]:hover { 
                background-color: #e2e8f0 !important; 
                color: #334155 !important;
            }
            .stTabs [aria-selected="true"] { 
                color: #1e40af !important; 
                background: #ffffff !important;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                font-weight: 600;
            }
            .stTabs [data-baseweb="tab-highlight"] { display: none; }
            .stTabs [data-baseweb="tab-border"] { display: none; }
            .stSelectbox label, .stMultiSelect label, .stTextInput label { color: #374151 !important; }
            header[data-testid="stHeader"] { background-color: #ffffff !important; }
            
            /* Botones secundarios con estilo claro */
            button[data-testid="stBaseButton-secondary"] { 
                background-color: #f1f5f9 !important; 
                color: #374151 !important; 
                border: 1px solid #e2e8f0 !important;
            }
            button[data-testid="stBaseButton-secondary"]:hover { 
                background-color: #e2e8f0 !important; 
                border-color: #cbd5e1 !important;
            }
            
            /* Botones primarios */
            button[data-testid="stBaseButton-primary"] { 
                background-color: #3b82f6 !important; 
                color: white !important;
            }
            button[data-testid="stBaseButton-primary"]:hover { 
                background-color: #2563eb !important;
            }
            
            /* Selectbox y inputs */
            .stSelectbox > div > div { 
                background-color: #f8fafc !important; 
                color: #1f2937 !important;
                border: 1px solid #e2e8f0 !important;
            }
            .stSelectbox [data-baseweb="select"] > div {
                background-color: #f8fafc !important;
                color: #1f2937 !important;
            }
            .stTextInput > div > div > input {
                background-color: #f8fafc !important;
                color: #1f2937 !important;
                border: 1px solid #e2e8f0 !important;
            }
            
            /* Dropdown menu del selectbox */
            [data-baseweb="popover"] {
                background-color: #ffffff !important;
            }
            [data-baseweb="menu"] {
                background-color: #ffffff !important;
            }
            [data-baseweb="menu"] li {
                background-color: #ffffff !important;
                color: #1f2937 !important;
            }
            [data-baseweb="menu"] li:hover {
                background-color: #f1f5f9 !important;
            }
            [role="listbox"] {
                background-color: #ffffff !important;
            }
            [role="option"] {
                background-color: #ffffff !important;
                color: #1f2937 !important;
            }
            [role="option"]:hover {
                background-color: #f1f5f9 !important;
            }
            [aria-selected="true"] {
                background-color: #e0f2fe !important;
            }
            
            /* Multiselect */
            .stMultiSelect > div > div {
                background-color: #ffffff !important;
                color: #1f2937 !important;
                border: 1px solid #d1d5db !important;
            }
            .stMultiSelect [data-baseweb="tag"] {
                background-color: #e0f2fe !important;
                color: #1f2937 !important;
            }
            .stMultiSelect [data-baseweb="input"] {
                background-color: #ffffff !important;
            }
            /* Force placeholder visibility in multiselect */
            .stMultiSelect > div > div > div {
                color: #6b7280 !important;
            }
            .stMultiSelect > div > div > div > div {
                color: #6b7280 !important;
            }
            .stMultiSelect [data-baseweb="select"] > div > div > div {
                color: #6b7280 !important;
            }
            [data-testid="stMultiSelect"] > div > div > div > div > div {
                color: #6b7280 !important;
                -webkit-text-fill-color: #6b7280 !important;
            }
            
            /* Force light backgrounds on all form elements */
            div[data-baseweb="select"] {
                background-color: #ffffff !important;
            }
            div[data-baseweb="select"] > div {
                background-color: #ffffff !important;
                color: #1f2937 !important;
            }
            div[data-baseweb="input"] {
                background-color: #ffffff !important;
            }
            div[data-baseweb="base-input"] {
                background-color: #ffffff !important;
                color: #1f2937 !important;
            }
            
            /* Dividers and containers */
            hr {
                border-color: #e5e7eb !important;
            }
            .stDivider {
                border-color: #e5e7eb !important;
            }
            
            /* DataFrame - Estilos consistentes con modo oscuro */
            /* DataFrame - Sin estilos personalizados para no interferir con el canvas */
            .stDataFrame { border-radius: 10px; overflow: hidden; }
            
            /* Expander */
            div[data-testid="stExpander"] { 
                background-color: #f8fafc !important; 
                border: 1px solid #e2e8f0 !important; 
                border-radius: 8px !important;
            }
            div[data-testid="stExpander"] summary { 
                color: #1f2937 !important; 
                background-color: #f1f5f9 !important;
            }
            div[data-testid="stExpander"] summary * { color: #1f2937 !important; }
            div[data-testid="stExpander"] summary p { color: #1f2937 !important; }
            div[data-testid="stExpander"] summary span { color: #1f2937 !important; }
            div[data-testid="stExpander"] summary div { color: #1f2937 !important; }
            div[data-testid="stExpander"] details summary span { color: #1f2937 !important; }
            div[data-testid="stExpander"] [data-testid="stExpanderDetails"] { background-color: #f8fafc !important; }
            .streamlit-expanderHeader { color: #1f2937 !important; background-color: #f1f5f9 !important; }
            .streamlit-expanderHeader * { color: #1f2937 !important; }
            details[data-testid="stExpander"] > summary { color: #1f2937 !important; background-color: #f1f5f9 !important; }
            details[data-testid="stExpander"] > summary * { color: #1f2937 !important; }
            [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p { color: #1f2937 !important; }
            
            /* Generic overrides for Streamlit dark theme elements */
            [data-testid="stToolbar"] {
                background-color: #ffffff !important;
            }
            .st-emotion-cache-1629p8f, .st-emotion-cache-1dp5vir {
                background-color: #ffffff !important;
            }
            .st-emotion-cache-16idsys p {
                color: #374151 !important;
            }
            
            /* Placeholder text styling */
            input::placeholder {
                color: #6b7280 !important;
                opacity: 1 !important;
            }
            .stTextInput input::placeholder {
                color: #6b7280 !important;
            }
            .stMultiSelect [data-baseweb="input"] input::placeholder {
                color: #6b7280 !important;
            }
            [data-baseweb="select"] [data-baseweb="input"] {
                color: #1f2937 !important;
            }
            /* Placeholder in multiselect */
            .stMultiSelect span[data-baseweb="tag"] ~ div input::placeholder,
            .stMultiSelect > div > div > div > div > input::placeholder {
                color: #6b7280 !important;
            }
            /* Fix gray text in select dropdowns */
            [data-baseweb="select"] span {
                color: #1f2937 !important;
            }
            .stSelectbox [data-baseweb="select"] > div > div {
                color: #1f2937 !important;
            }
            /* Placeholder span in multiselect */
            .stMultiSelect [data-baseweb="input"] > div {
                color: #6b7280 !important;
            }
            /* Fix transparent placeholder in multiselect */
            .stMultiSelect [data-baseweb="input"] [data-baseweb="base-input"] {
                background-color: #ffffff !important;
            }
            .stMultiSelect [data-baseweb="combobox"] > div {
                background-color: #ffffff !important;
            }
            .stMultiSelect div[data-baseweb="popover"] {
                background-color: #ffffff !important;
            }
            .stMultiSelect [aria-label] {
                color: #6b7280 !important;
                opacity: 1 !important;
            }
            /* Placeholder text visibility */
            [data-testid="stMultiSelect"] span {
                color: #6b7280 !important;
                opacity: 1 !important;
            }
            [data-testid="stMultiSelect"] [data-baseweb="input"] span {
                color: #6b7280 !important;
            }
        </style>
        """

st.markdown(get_theme_styles(), unsafe_allow_html=True)

st.markdown("""
    <style>
        [data-testid="stMainBlockContainer"] > div:first-child [data-testid="stImage"] img {
            background: white;
            border-radius: 8px;
            padding: 8px;
        }
        /* Ocultar textos en inglés de Streamlit dataframe */
        [data-testid="stDataFrame"] input::placeholder {
            color: transparent !important;
        }
        [data-testid="stDataFrame"] input:focus::placeholder {
            color: transparent !important;
        }
        /* Estilo para inputs de navegación de dataframe */
        [data-testid="stDataFrame"] input {
            text-align: center !important;
            font-weight: 500 !important;
        }
    </style>
""", unsafe_allow_html=True)

col_logo, col_title, col_sync, col_help, col_theme = st.columns([1.2, 5, 2, 0.6, 0.6])
with col_logo:
    st.image("static/logo.png", width="stretch")
with col_title:
    st.markdown('<h1 class="main-header" style="margin: 0; padding-top: 0.5rem; letter-spacing: 0.5px; white-space: nowrap;">Reposición de Sucursales</h1>', unsafe_allow_html=True)
with col_sync:
    last_sync = db.get_last_sync()
    if last_sync and last_sync.get('timestamp'):
        ts = last_sync['timestamp']
        from datetime import timedelta
        if hasattr(ts, 'strftime'):
            ts_argentina = ts - timedelta(hours=3)
            fecha_str = ts_argentina.strftime("%d/%m/%y %H:%M")
        else:
            fecha_str = str(ts)[:16]
        text_color = "#e2e8f0" if st.session_state.dark_mode else "#1f2937"
        st.markdown(f"""
        <div style="text-align: right; padding-top: 0.3rem;">
            <span style="font-size: 1rem; font-weight: 600; color: {text_color};">{fecha_str}</span><br>
            <span style="font-size: 0.7rem; opacity: 0.8; color: {text_color};">Última actualización</span>
        </div>
        """, unsafe_allow_html=True)
with col_help:
    st.markdown("<div style='padding-top: 0.3rem;'></div>", unsafe_allow_html=True)
    if st.button("❓", key="help_toggle", help="Tour guiado del sistema"):
        st.session_state.show_tour = not st.session_state.get("show_tour", False)
        st.rerun()

with col_theme:
    st.markdown("<div style='padding-top: 0.3rem;'></div>", unsafe_allow_html=True)
    tema_actual = st.session_state.dark_mode
    icono = "☀️" if tema_actual else "🌙"
    tooltip = "Cambiar a modo claro" if tema_actual else "Cambiar a modo oscuro"
    if st.button(icono, key="theme_toggle", help=tooltip):
        st.session_state.dark_mode = not tema_actual
        st.rerun()

# Tour Guiado Profesional con Overlay
TOUR_STEPS = [
    {
        "titulo": "Bienvenido al Sistema",
        "icono": "👋",
        "contenido": "Este es tu panel de control para gestionar stock y reposición de todas las sucursales de La Tijera.",
        "tip": "Usá este tour para conocer las funciones principales."
    },
    {
        "titulo": "Tarjetas de Sucursales",
        "icono": "🏪",
        "contenido": "Cada tarjeta representa una sucursal. El color del borde indica el estado general del stock.",
        "tip": "🔴 Crítico  •  🟡 Atención  •  🟢 Normal  •  🟠 Exceso"
    },
    {
        "titulo": "Métricas y Detalle",
        "icono": "📊",
        "contenido": "Dentro de cada tarjeta verás artículos faltantes, valor a reponer y alertas. Hacé clic en 'Ver Detalle' para más información.",
        "tip": "El detalle muestra cada artículo con su stock y proyección."
    },
    {
        "titulo": "Pestaña Distribución",
        "icono": "📈",
        "contenido": "En la segunda pestaña encontrarás la matriz de redistribución: qué artículos enviar desde el depósito central a cada sucursal.",
        "tip": "Filtrá por familia o artículo para análisis específicos."
    },
    {
        "titulo": "Alertas y Notificaciones",
        "icono": "🔔",
        "contenido": "Podés enviar alertas por Email o WhatsApp a los responsables de cada sucursal cuando hay stock crítico.",
        "tip": "📧 Email  •  📲 WhatsApp"
    }
]

def render_tour():
    """Renderiza el tour guiado con modal centrado y botones Streamlit"""
    if not st.session_state.get("show_tour", False):
        return
    
    if "tour_step" not in st.session_state:
        st.session_state.tour_step = 0
    
    step = st.session_state.tour_step
    total = len(TOUR_STEPS)
    paso = TOUR_STEPS[step]
    
    # Generar dots de progreso
    dots_html = "".join([
        f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
        f'background:{"#3b82f6" if i == step else "#cbd5e1"};margin:0 4px;'
        f'{"transform:scale(1.2);" if i == step else ""}"></span>' 
        for i in range(total)
    ])
    
    # Ocultar sidebar y aplicar estilos de tour
    st.markdown("""
    <style>
    [data-testid="stSidebar"] { display: none !important; }
    [data-testid="stHeader"] { display: none !important; }
    [data-testid="stAppViewContainer"] {
        background: rgba(0,0,0,0.9) !important;
    }
    [data-testid="stMainBlockContainer"] {
        max-width: 460px !important;
        margin: 0 auto !important;
        padding-top: 5vh !important;
    }
    .tour-card {
        background: white;
        border-radius: 16px;
        box-shadow: 0 25px 80px rgba(0,0,0,0.4);
        overflow: hidden;
    }
    .tour-header {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
        padding: 28px 24px;
        text-align: center;
        color: white;
    }
    .tour-icon { font-size: 2.5rem; margin-bottom: 10px; }
    .tour-title { font-size: 1.3rem; font-weight: 700; margin: 0; }
    .tour-body { padding: 24px; }
    .tour-content { color: #374151; font-size: 1rem; line-height: 1.7; margin-bottom: 16px; }
    .tour-tip {
        background: #eff6ff;
        border-left: 4px solid #3b82f6;
        padding: 14px 16px;
        border-radius: 0 8px 8px 0;
        color: #1e40af;
        font-size: 0.9rem;
    }
    .tour-footer {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 14px 24px;
        background: #f8fafc;
        border-top: 1px solid #e2e8f0;
    }
    .tour-counter { color: #64748b; font-size: 0.9rem; font-weight: 600; }
    </style>
    """, unsafe_allow_html=True)
    
    # Card del tour
    st.markdown(f'''
    <div class="tour-card">
        <div class="tour-header">
            <div class="tour-icon">{paso["icono"]}</div>
            <h2 class="tour-title">{paso["titulo"]}</h2>
        </div>
        <div class="tour-body">
            <p class="tour-content">{paso["contenido"]}</p>
            <div class="tour-tip">{paso["tip"]}</div>
        </div>
        <div class="tour-footer">
            <span class="tour-counter">{step + 1} de {total}</span>
            <div>{dots_html}</div>
        </div>
    </div>
    ''', unsafe_allow_html=True)
    
    # Espaciado
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    
    # Botones nativos de Streamlit
    col1, col2 = st.columns(2)
    
    with col1:
        if step > 0:
            if st.button("◀ Anterior", key="tour_prev", use_container_width=True):
                st.session_state.tour_step = step - 1
                st.rerun()
        else:
            if st.button("✖ Saltar Tour", key="tour_skip", use_container_width=True):
                st.session_state.show_tour = False
                st.session_state.tour_step = 0
                st.rerun()
    
    with col2:
        if step < total - 1:
            if st.button("Siguiente ▶", key="tour_next", type="primary", use_container_width=True):
                st.session_state.tour_step = step + 1
                st.rerun()
        else:
            if st.button("✅ Comenzar", key="tour_finish", type="primary", use_container_width=True):
                st.session_state.show_tour = False
                st.session_state.tour_step = 0
                st.rerun()
    
    # Detener el resto de la página
    st.stop()


def get_alerta_color(alerta):
    colores = {
        "Quiebre de stock": "🔴",
        "Stock de Seguridad": "🔵",
        "Pto de Pedido": "🟣",
        "OK": "🟢",
        "Sobre stock": "🟡",
        "Sin rotación (sin stock)": "⚫",
        "Sin rotación (con sobrestock)": "🟠"
    }
    return colores.get(alerta, "⚪")

def get_alerta_style(alerta):
    if st.session_state.dark_mode:
        estilos = {
            "Quiebre de stock": "background-color: #7f1d1d; color: #fecaca;",
            "Stock de Seguridad": "background-color: #1e3a5f; color: #93c5fd;",
            "Pto de Pedido": "background-color: #0d5457; color: #99f6e4;",
            "OK": "background-color: #14532d; color: #86efac;",
            "Sobre stock": "background-color: #78350f; color: #fde68a;",
            "Sin rotación (sin stock)": "background-color: #4b5563; color: #d1d5db;",
            "Sin rotación (con sobrestock)": "background-color: #7c2d12; color: #fed7aa;"
        }
    else:
        estilos = {
            "Quiebre de stock": "background-color: #fee2e2; color: #991b1b;",
            "Stock de Seguridad": "background-color: #dbeafe; color: #1e40af;",
            "Pto de Pedido": "background-color: #cffafe; color: #155e75;",
            "OK": "background-color: #dcfce7; color: #166534;",
            "Sobre stock": "background-color: #fef3c7; color: #92400e;",
            "Sin rotación (sin stock)": "background-color: #f3f4f6; color: #374151;",
            "Sin rotación (con sobrestock)": "background-color: #ffedd5; color: #9a3412;"
        }
    return estilos.get(alerta, "")

# Mostrar tour guiado si está activo (fuera de tabs para overlay completo)
render_tour()

tab_resumen, tab_distribucion, tab_costos, tab_alertas = st.tabs(["📋 Resumen", "🔄 Distribución", "💰 Costos", "📲 Alertas"])

with tab_resumen:
    
    st.markdown("""
    <style>
    .resumen-card {
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .card-red { background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%); color: white; }
    .card-red .card-value { color: white !important; }
    .card-yellow { background: linear-gradient(135deg, #facc15 0%, #eab308 100%); color: #1f2937; }
    .card-yellow .card-value { color: #1f2937 !important; }
    .card-green { background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); color: white; }
    .card-green .card-value { color: white !important; }
    .card-title { font-size: 1rem; font-weight: 700; margin-bottom: 0.4rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .card-summary { margin-bottom: 0.5rem; }
    .summary-row { font-size: 0.85rem; margin: 0.15rem 0; }
    .card-row { display: flex; align-items: center; gap: 0.4rem; margin: 0.3rem 0; font-size: 0.85rem; flex-wrap: wrap; }
    .card-value { font-weight: 700; }
    .alert-table { 
        width: 100%;
        font-size: 0.72rem; 
        margin-top: 0.4rem;
        border-top: 1px solid rgba(255,255,255,0.3);
        padding-top: 0.4rem;
        border-collapse: collapse;
    }
    .alert-table td { padding: 0.15rem 0; }
    .alert-table .icon { width: 20px; }
    .alert-table .name { text-align: left; }
    .alert-table .num { text-align: right; font-weight: 700; width: 45px; }
    .alert-table .money { text-align: right; width: 55px; font-size: 0.68rem; }
    .card-red .alert-table, .card-green .alert-table { color: white; }
    .card-yellow .alert-table { border-top-color: rgba(0,0,0,0.2); color: #1f2937; }
    .card-btn {
        display: inline-block;
        margin-top: 0.8rem;
        padding: 0.4rem 1rem;
        background: white;
        color: #374151;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.8rem;
        text-decoration: none;
        box-shadow: 0 2px 6px rgba(0,0,0,0.2);
        cursor: pointer;
        align-self: flex-start;
    }
    @media (max-width: 1200px) {
        .resumen-card { min-height: 110px; padding: 0.9rem; }
        .card-title { font-size: 0.9rem; }
        .alert-grid { font-size: 0.7rem; grid-template-columns: repeat(2, 1fr); }
    }
    @media (max-width: 768px) {
        .resumen-card { min-height: 100px; padding: 0.7rem; }
        .card-title { font-size: 0.85rem; }
        .alert-grid { font-size: 0.65rem; }
        .card-btn { font-size: 0.75rem; padding: 0.3rem 0.8rem; }
    }
    .resumen-table {
        width: 100%;
        border-collapse: collapse;
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-top: 1rem;
    }
    .resumen-table th {
        background: #f3f4f6;
        color: #374151;
        font-weight: 600;
        padding: 0.8rem;
        text-align: left;
        border-bottom: 2px solid #e5e7eb;
    }
    .resumen-table td {
        padding: 0.7rem 0.8rem;
        border-bottom: 1px solid #e5e7eb;
        color: #374151;
    }
    .resumen-table tr:hover { background: #f9fafb; }
    .pagination {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 0.5rem;
        margin-top: 0.5rem;
        padding: 0.75rem 1rem;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        background: #f9fafb;
        width: 100%;
        box-sizing: border-box;
    }
    .page-btn {
        padding: 0.4rem 0.8rem;
        border: 1px solid #d1d5db;
        background: white;
        color: #374151;
        border-radius: 4px;
        font-size: 0.85rem;
        cursor: pointer;
    }
    .page-btn:hover { background: #f3f4f6; }
    .page-btn.active { background: #3b82f6; color: white; border-color: #3b82f6; }
    </style>
    """, unsafe_allow_html=True)
    
    FRANQUICIAS = ["LUJAN", "MAIPU", "SAN RAFAEL", "SMARTIN", "TUNUYAN"]
    
    resumen_data = db.get_resumen_reposicion()
    
    if resumen_data and len(resumen_data) > 0:
        cards_data = resumen_data.get('cards', [])
        tabla_data = resumen_data.get('tabla', [])
        
        def es_franquicia(sucursal):
            suc_upper = sucursal.upper().replace("LA TIJERA ", "")
            return any(f in suc_upper for f in FRANQUICIAS)
        
        cards_propias = [c for c in cards_data if not es_franquicia(c.get('sucursal', ''))]
        cards_franquicias = [c for c in cards_data if es_franquicia(c.get('sucursal', ''))]
        
        def render_cards_con_detalle(cards_list, titulo, icon, es_franquicia_grupo):
            st.markdown(f"### {icon} {titulo}")
            st.caption("Cada tarjeta muestra: valor total a reponer, cantidad de artículos en quiebre o stock de seguridad, y grupos/categorías afectadas")
            if not cards_list:
                st.info(f"No hay artículos críticos en {titulo.lower()}")
                return
            
            num_cards = len(cards_list)
            cols = st.columns(min(num_cards, 5))
            
            for i, card in enumerate(cards_list[:5]):
                with cols[i % len(cols)]:
                    valor_critico = card.get('quiebre_val', 0) + card.get('seguridad_val', 0)
                    if valor_critico > 50000000:
                        card_class = 'card-red'
                    elif valor_critico > 20000000:
                        card_class = 'card-yellow'
                    else:
                        card_class = 'card-green'
                    
                    sucursal = card.get('sucursal', 'N/A')
                    
                    def fmt_val(v):
                        if v >= 1000000:
                            return f"${v/1000000:.1f}M"
                        elif v >= 1000:
                            return f"${v/1000:.0f}K"
                        return f"${v:.0f}"
                    
                    q_qty = card.get('quiebre_qty', 0)
                    q_val = fmt_val(card.get('quiebre_val', 0))
                    s_qty = card.get('seguridad_qty', 0)
                    s_val = fmt_val(card.get('seguridad_val', 0))
                    p_qty = card.get('pedido_qty', 0)
                    p_val = fmt_val(card.get('pedido_val', 0))
                    sob_qty = card.get('sobrestock_qty', 0)
                    sob_val = fmt_val(card.get('sobrestock_val', 0))
                    sin_qty = card.get('sinrot_qty', 0)
                    ok_qty = card.get('ok_qty', 0)
                    
                    total_critico = card.get('quiebre_val', 0) + card.get('seguridad_val', 0)
                    total_critico_fmt = f"${total_critico:,.0f}".replace(",", ".")
                    art_criticos = q_qty + s_qty
                    
                    st.markdown(f"""
                    <div class="resumen-card {card_class}">
                        <div class="card-title">{sucursal}</div>
                        <div class="card-summary">
                            <div class="summary-row">💰 <b>{total_critico_fmt}</b></div>
                            <div class="summary-row">📦 {art_criticos} art. críticos</div>
                        </div>
                        <table class="alert-table">
                            <tr title="Quiebre de stock: artículos sin stock disponible"><td class="icon">🚨</td><td class="name">Quiebre</td><td class="num">{q_qty}</td><td class="money">{q_val}</td></tr>
                            <tr title="Stock de Seguridad: artículos con stock bajo (1-2 meses)"><td class="icon">⚠️</td><td class="name">Seguridad</td><td class="num">{s_qty}</td><td class="money">{s_val}</td></tr>
                            <tr title="Punto de Pedido: artículos que necesitan reposición pronto"><td class="icon">📋</td><td class="name">Pedido</td><td class="num">{p_qty}</td><td class="money">{p_val}</td></tr>
                            <tr title="Sobrestock: artículos con exceso de stock (>6 meses)"><td class="icon">📦</td><td class="name">Sobrestock</td><td class="num">{sob_qty}</td><td class="money">{sob_val}</td></tr>
                            <tr title="Sin rotación: artículos sin ventas registradas"><td class="icon">💤</td><td class="name">Sin rot.</td><td class="num">{sin_qty}</td><td class="money">-</td></tr>
                            <tr title="OK: artículos con stock normal (2-6 meses)"><td class="icon">✅</td><td class="name">OK</td><td class="num">{ok_qty}</td><td class="money">-</td></tr>
                        </table>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    btn_key = f"detalle_{sucursal}_{i}"
                    current_state = st.session_state.get("sucursal_activa", None)
                    is_active = current_state == f"{sucursal}_{es_franquicia_grupo}"
                    
                    btn_label = "❌ Ocultar" if is_active else "📋 Ver Detalle"
                    if st.button(btn_label, key=btn_key, use_container_width=True):
                        if is_active:
                            st.session_state["sucursal_activa"] = None
                        else:
                            st.session_state["sucursal_activa"] = f"{sucursal}_{es_franquicia_grupo}"
                        st.rerun()
            
            sucursal_activa = st.session_state.get("sucursal_activa", None)
            if sucursal_activa:
                for card in cards_list[:5]:
                    sucursal = card.get('sucursal', 'N/A')
                    if sucursal_activa == f"{sucursal}_{es_franquicia_grupo}":
                        lista_precio = "102" if es_franquicia_grupo else "2"
                        
                        st.markdown(f"#### 📦 Detalle: {sucursal} (Lista {lista_precio})")
                        
                        periodo = st.radio(
                            "Período de proyección:",
                            [15, 30, 60],
                            horizontal=True,
                            key=f"periodo_{sucursal}",
                            format_func=lambda x: f"{x} días"
                        )
                        
                        detalle = db.get_detalle_sucursal(sucursal, lista_precio, periodo)
                        
                        if detalle and len(detalle) > 0:
                            df_detalle = pd.DataFrame(detalle)
                            
                            total_valor = df_detalle['valor'].sum()
                            total_faltante = df_detalle['faltante'].sum()
                            
                            col_t1, col_t2, col_t3 = st.columns(3)
                            with col_t1:
                                st.metric(
                                    "Total Artículos", 
                                    len(df_detalle),
                                    help="Cantidad de artículos con faltante en esta sucursal"
                                )
                            with col_t2:
                                st.metric(
                                    "Unidades Faltantes", 
                                    f"{total_faltante:,.0f}".replace(",", "."),
                                    help=f"Total de unidades a reponer para cubrir {periodo} días de venta"
                                )
                            with col_t3:
                                st.metric(
                                    "Valor a Reponer", 
                                    f"${total_valor:,.0f}".replace(",", "."),
                                    help=f"Inversión necesaria para reponer stock de {periodo} días (Lista {lista_precio})"
                                )
                            
                            df_display = df_detalle.copy()
                            df_display['valor'] = df_display['valor'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
                            df_display['precio'] = df_display['precio'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
                            df_display.columns = ['Código', 'Descripción', 'Stock', 'Vta Diaria', 'Necesidad', 'Faltante', 'Precio', 'Valor $']
                            
                            st.dataframe(df_display, width="stretch", hide_index=True)
                            
                            output = BytesIO()
                            df_detalle.to_excel(output, index=False, engine='openpyxl')  # type: ignore
                            st.download_button(
                                "📥 Exportar Excel",
                                output.getvalue(),
                                file_name=f"detalle_{sucursal}_{periodo}dias.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"excel_{sucursal}"
                            )
                        else:
                            st.info("No hay artículos críticos para este período.")
        
        render_cards_con_detalle(cards_propias, "Sucursales Propias", "🏢", False)
        st.markdown("---")
        render_cards_con_detalle(cards_franquicias, "Franquicias", "🏪", True)
        
        st.markdown("---")
        st.markdown("### 📋 Reposición Consolidada")
        st.caption("Resumen por sucursal y grupo de productos: cantidad de artículos críticos, unidades faltantes y valor estimado de reposición")
        
        dias_consolidado = st.radio(
            "Período de proyección:",
            options=[15, 30, 60],
            format_func=lambda x: f"{x} días",
            horizontal=True,
            key="dias_consolidado"
        )
        
        resumen_filtrado = db.get_resumen_reposicion(dias=dias_consolidado)
        tabla_data = resumen_filtrado.get('tabla', [])
        
        if tabla_data:
            df_consolidado = pd.DataFrame(tabla_data)
            df_consolidado['valor'] = df_consolidado['valor'].apply(lambda x: f"${x:,.0f}".replace(",", "."))
            df_consolidado['cant_reponer'] = df_consolidado['cant_reponer'].apply(lambda x: f"{x:,.0f}".replace(",", "."))
            df_consolidado.columns = ['Sucursal', 'Categoría', 'Artículos', 'Faltantes', 'Cant. a Reponer', 'Valor $']
            
            st.dataframe(df_consolidado, width="stretch", hide_index=True, height=400)
            
            total_valor = sum([row.get('valor', 0) for row in tabla_data])
            total_articulos = sum([row.get('articulos', 0) for row in tabla_data])
            
            col_tot1, col_tot2 = st.columns(2)
            with col_tot1:
                st.metric("Total Artículos Críticos", f"{total_articulos:,}".replace(",", "."))
            with col_tot2:
                st.metric("Valor Total a Reponer", f"${total_valor:,.0f}".replace(",", "."))
        else:
            st.info("No hay datos de reposición consolidada disponibles.")
    else:
        st.markdown("""
        <div class="resumen-card card-red">
            <div class="card-title">MAIPU</div>
            <div class="card-row">💰 Valor: <span class="card-value">$1.245.300</span></div>
            <div class="card-row">📦 Art. críticos: <span class="card-value">12</span></div>
            <div class="card-row">🏷️ Categorías: <span class="card-value">4</span></div>
            <div class="card-btn">Ver Detalle</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.info("Sincronice los datos para ver el resumen de reposición actualizado.")

# TAB ANÁLISIS OCULTO TEMPORALMENTE
if False:  # with tab_analisis:
    col1, col2, col3 = st.columns([2, 2, 3])
    
    with col1:
        sucursales = ["Todas"] + db.get_sucursales()
        
        def acortar_sucursal(nombre):
            if nombre == "Todas":
                return "Todas"
            if nombre == "CRISA 2":
                return "CRISA 2"
            if nombre == "CRISA CENTRAL":
                return "CDD"
            return nombre.replace("LA TIJERA ", "")
        
        sucursal_filter = st.selectbox(
            "🏢 Sucursal", 
            sucursales, 
            key="sucursal",
            format_func=acortar_sucursal
        )
    
    with col2:
        alertas_opciones = ["Todas", "Quiebre de stock", "Stock de Seguridad", "Pto de Pedido", "OK", "Sobre stock", "Sin rotación (sin stock)", "Sin rotación (con sobrestock)"]
        alerta_filter = st.selectbox("🚨 Tipo de Alerta", alertas_opciones, key="alerta")
    
    with col3:
        buscar_texto = st.text_input(
            "🔍 Buscar artículo (usa * para colores)", 
            key="buscar_articulos_texto",
            placeholder="Ej: TA148L* busca 00-32"
        )
        articulos_seleccionados = []

    col_cat1, col_cat2 = st.columns(2)
    
    with col_cat1:
        categorias_disponibles = db.get_categorias()
        categorias_filter = st.multiselect("📁 Categoría", categorias_disponibles, key="categoria", placeholder="Seleccionar categorías...")
    
    with col_cat2:
        if categorias_filter:
            subcategorias_disponibles = []
            for cat in categorias_filter:
                subcategorias_disponibles.extend(db.get_subcategorias(cat))
            subcategorias_disponibles = list(set(subcategorias_disponibles))
            subcategorias_disponibles.sort()
        else:
            subcategorias_disponibles = db.get_subcategorias()
        subcategorias_filter = st.multiselect("📂 Subcategoría", subcategorias_disponibles, key="subcategoria", placeholder="Seleccionar subcategorías...")

    col_periodo, col_empty = st.columns([2, 5])
    with col_periodo:
        periodos_opciones = {
            "15 días próximos": 15,
            "30 días próximos": 30,
            "60 días próximos": 60,
            "90 días próximos": 90,
            "365 días próximos": 365
        }
        periodo_seleccionado = st.selectbox(
            "📅 Período de proyección",
            options=list(periodos_opciones.keys()),
            index=2,
            key="periodo_proyeccion"
        )
        dias_proyeccion = periodos_opciones[periodo_seleccionado]

    st.divider()

    sucursal_param = sucursal_filter if sucursal_filter != "Todas" else None
    alerta_param = alerta_filter if alerta_filter != "Todas" else None

    metricas = db.get_metricas(sucursal_param, alerta_param)

    if not metricas:
        st.warning("⚠️ No hay datos sincronizados todavía.")
        
        with st.expander("📖 Instrucciones de configuración", expanded=True):
            st.markdown("""
            ### Para comenzar a usar el sistema:
            
            1. **Descarga el archivo `bridge_sql.py`** a tu PC local
            2. **Configura la URL de Replit** en la variable `REPL_URL`
            3. **Ejecuta el script**: `python bridge_sql.py`
            4. Los datos se sincronizarán automáticamente cada 60 segundos
            
            ---
            
            **Conexión SQL Server:**
            - Servidor: `tangoserver`
            - Base de datos: `crisa_real1`
            - Usuario: `Axoft`
            """)
    else:
        df = pd.DataFrame(metricas)
        
        if buscar_texto and buscar_texto.strip():
            terminos = [t.strip().upper() for t in buscar_texto.split(",") if t.strip()]
            if terminos:
                mask = pd.Series([False] * len(df))
                for termino in terminos:
                    if termino.endswith("*"):
                        codigo_base = termino[:-1]
                        mask_base = df["cod_articulo"].astype(str).str.upper().str.startswith(codigo_base)
                        mask = mask | mask_base
                    else:
                        mask_cod = df["cod_articulo"].astype(str).str.upper().str.contains(termino, na=False)
                        mask_desc = df["descripcion"].astype(str).str.upper().str.contains(termino, na=False)
                        mask_fam = df["familia"].astype(str).str.upper().str.contains(termino, na=False) if "familia" in df.columns else pd.Series([False] * len(df))
                        mask_desc_fam = df["desc_familia"].astype(str).str.upper().str.contains(termino, na=False) if "desc_familia" in df.columns else pd.Series([False] * len(df))
                        mask = mask | mask_cod | mask_desc | mask_fam | mask_desc_fam
                df = df[mask]
        
        if categorias_filter or subcategorias_filter:
            articulos_filtrados = db.get_articulos_por_categoria(categorias_filter, subcategorias_filter)
            if articulos_filtrados:
                df = df[df["cod_articulo"].isin(articulos_filtrados)]
            else:
                df = df.iloc[0:0]
        
        df = df[df["stock_1"] > 0]
        df = df.sort_values("stock_1", ascending=False)
        
        if "venta_promedio_diaria" in df.columns:
            df["venta_promedio_diaria"] = pd.to_numeric(df["venta_promedio_diaria"], errors='coerce').fillna(0)
            df["stock_1"] = pd.to_numeric(df["stock_1"], errors='coerce').fillna(0)
            df["necesidad_periodo"] = (df["venta_promedio_diaria"] * dias_proyeccion) - df["stock_1"]
            df["necesidad_periodo"] = df["necesidad_periodo"].round(0)
            import math
            df["pedido_periodo"] = df["necesidad_periodo"].apply(lambda x: max(0, math.ceil(x)) if pd.notna(x) and x > 0 else 0)
        
        df["alerta_icon"] = df["alerta_stock"].apply(get_alerta_color)
        df["alerta_display"] = df["alerta_icon"] + " " + df["alerta_stock"]
        
        totales = db.get_totales(sucursal_param)
        alertas_count = db.get_alertas_count(sucursal_param)
        
        with st.expander("📈 Resumen General", expanded=False):
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            
            with col_m1:
                st.metric(
                    label="Total Artículos",
                    value=f"{len(df):,}",
                    delta=f"de {totales['total_articulos']:,}" if alerta_param or buscar_texto else None
                )
            
            with col_m2:
                stock_val = float(df["stock_1"].sum()) if not df.empty else 0
                st.metric(
                    label="Stock Total",
                    value=f"{stock_val:,.0f}"
                )
            
            with col_m3:
                venta_val = float(df["total_venta"].sum()) if "total_venta" in df.columns and not df.empty else 0
                st.metric(
                    label="Venta Total Período",
                    value=f"{venta_val:,.0f}"
                )
            
            with col_m4:
                quiebres = len(df[df["alerta_stock"] == "Quiebre"]) if not df.empty else 0
                st.metric(
                    label="En Quiebre",
                    value=quiebres,
                    delta="Crítico" if quiebres > 0 else "OK",
                    delta_color="inverse" if quiebres > 0 else "normal"
                )
        
        with st.expander("🚨 Estado de Alertas", expanded=False):
            st.markdown("""
            <style>
                @keyframes pulse-green {
                    0%, 100% { box-shadow: 0 0 5px #10b981, 0 0 10px #10b981, 0 0 15px #10b981; }
                    50% { box-shadow: 0 0 10px #10b981, 0 0 20px #10b981, 0 0 30px #10b981; }
                }
                @keyframes pulse-yellow {
                    0%, 100% { box-shadow: 0 0 5px #f59e0b, 0 0 10px #f59e0b, 0 0 15px #f59e0b; }
                    50% { box-shadow: 0 0 10px #f59e0b, 0 0 20px #f59e0b, 0 0 30px #f59e0b; }
                }
                @keyframes pulse-red {
                    0%, 100% { box-shadow: 0 0 5px #ef4444, 0 0 10px #ef4444, 0 0 15px #ef4444; }
                    50% { box-shadow: 0 0 10px #ef4444, 0 0 20px #ef4444, 0 0 30px #ef4444; }
                }
                .alert-card {
                    border-radius: 12px;
                    padding: 1rem;
                    text-align: center;
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                    cursor: pointer;
                    min-height: 120px;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                }
                .alert-card:hover {
                    transform: translateY(-5px);
                }
                .alert-value {
                    font-size: clamp(1.2rem, 3vw, 2rem);
                    font-weight: 800;
                    letter-spacing: -1px;
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    max-width: 100%;
                }
                .alert-label {
                    font-size: clamp(0.6rem, 1.5vw, 0.8rem);
                    font-weight: 600;
                    opacity: 0.9;
                    margin-top: 6px;
                    text-transform: uppercase;
                    white-space: nowrap;
                }
                .semaforo-light {
                    width: 20px; height: 20px;
                    border-radius: 50%;
                    margin: 0 auto 10px;
                }
            </style>
            """, unsafe_allow_html=True)
            
            sin_rotacion_total = alertas_count.get("Sin rotación (sin stock)", 0) + alertas_count.get("Sin rotación (con sobrestock)", 0)
            
            alertas_config = [
                ("OK", "#10b981", "linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%)", "#065f46", alertas_count.get("OK", 0), "pulse-green"),
                ("Pto Pedido", "#0891b2", "linear-gradient(135deg, #cffafe 0%, #a5f3fc 100%)", "#155e75", alertas_count.get("Pto de Pedido", 0), "none"),
                ("Stk Seguridad", "#eab308", "linear-gradient(135deg, #fef9c3 0%, #fde68a 100%)", "#713f12", alertas_count.get("Stock de Seguridad", 0), "pulse-yellow"),
                ("Quiebre", "#dc2626", "linear-gradient(135deg, #fecaca 0%, #fca5a5 100%)", "#7f1d1d", alertas_count.get("Quiebre de stock", 0), "pulse-red"),
                ("Sobrestock", "#f97316", "linear-gradient(135deg, #ffedd5 0%, #fed7aa 100%)", "#9a3412", alertas_count.get("Sobre stock", 0), "pulse-yellow"),
                ("Sin Rotación", "#6b7280", "linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 100%)", "#374151", sin_rotacion_total, "none")
            ]
            
            if st.session_state.dark_mode:
                alertas_config = [
                    ("OK", "#10b981", "linear-gradient(135deg, #064e3b 0%, #065f46 100%)", "#a7f3d0", alertas_count.get("OK", 0), "pulse-green"),
                    ("Pto Pedido", "#0891b2", "linear-gradient(135deg, #0c4a6e 0%, #155e75 100%)", "#a5f3fc", alertas_count.get("Pto de Pedido", 0), "none"),
                    ("Stk Seguridad", "#eab308", "linear-gradient(135deg, #422006 0%, #713f12 100%)", "#fde68a", alertas_count.get("Stock de Seguridad", 0), "pulse-yellow"),
                    ("Quiebre", "#dc2626", "linear-gradient(135deg, #7f1d1d 0%, #991b1b 100%)", "#fca5a5", alertas_count.get("Quiebre de stock", 0), "pulse-red"),
                    ("Sobrestock", "#f97316", "linear-gradient(135deg, #431407 0%, #7c2d12 100%)", "#fed7aa", alertas_count.get("Sobre stock", 0), "pulse-yellow"),
                    ("Sin Rotación", "#6b7280", "linear-gradient(135deg, #374151 0%, #4b5563 100%)", "#d1d5db", sin_rotacion_total, "none")
                ]
            
            cols = st.columns(6)
            for i, (nombre, color_accent, bg_gradient, text_color, count, animation) in enumerate(alertas_config):
                with cols[i]:
                    st.markdown(f"""
                    <div class="alert-card" style="
                        background: {bg_gradient};
                        border: 2px solid {color_accent};
                        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
                    ">
                        <div class="semaforo-light" style="
                            background: {color_accent};
                            animation: {animation} 2s ease-in-out infinite;
                        "></div>
                        <div class="alert-value" style="color: {text_color};">{count:,}</div>
                        <div class="alert-label" style="color: {text_color};">{nombre}</div>
                    </div>
                    """, unsafe_allow_html=True)
        
        filtros_activos = []
        if sucursal_filter != "Todas":
            filtros_activos.append(f"Sucursal: {sucursal_filter}")
        if alerta_filter != "Todas":
            filtros_activos.append(f"Alerta: {alerta_filter}")
        if categorias_filter:
            filtros_activos.append(f"Categorías: {len(categorias_filter)}")
        if subcategorias_filter:
            filtros_activos.append(f"Subcategorías: {len(subcategorias_filter)}")
        if buscar_texto:
            filtros_activos.append(f"Búsqueda: {buscar_texto}")
        
        if filtros_activos:
            estado_filtro = f"📌 Datos filtrados: {' | '.join(filtros_activos)}"
        else:
            estado_filtro = "📊 Mostrando todos los datos (sin filtros)"
        
        with st.expander(f"📊 Visualizaciones - {estado_filtro}", expanded=False):
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                alertas_data = []
                colores_alertas = {
                    "OK": "#10b981",
                    "Pto de Pedido": "#0891b2",
                    "Stock de Seguridad": "#eab308", 
                    "Quiebre de stock": "#dc2626",
                    "Sobre stock": "#f97316",
                    "Sin rotación (sin stock)": "#6b7280",
                    "Sin rotación (con sobrestock)": "#8b5cf6"
                }
                for alerta_nombre, count in alertas_count.items():
                    if count > 0:
                        alertas_data.append({
                            "Alerta": alerta_nombre,
                            "Cantidad": count,
                            "Color": colores_alertas.get(alerta_nombre, "#888888")
                        })
                
                if alertas_data:
                    df_alertas = pd.DataFrame(alertas_data)
                    total = df_alertas["Cantidad"].sum()
                    df_alertas["Porcentaje"] = (df_alertas["Cantidad"] / total * 100).round(1)
                    df_alertas = df_alertas.sort_values("Cantidad", ascending=False)
                    
                    fig_bar_h = px.bar(
                        df_alertas,
                        y="Alerta",
                        x="Cantidad",
                        title="Distribución de Alertas",
                        color="Alerta",
                        color_discrete_map=colores_alertas,
                        text="Cantidad",
                        orientation='h'
                    )
                    fig_bar_h.update_traces(
                        texttemplate='%{text:,.0f} (%{customdata:.1f}%)',
                        textposition='outside',
                        customdata=df_alertas["Porcentaje"],
                        textfont_size=11,
                        marker=dict(line=dict(width=0), cornerradius=4),
                        hovertemplate='<b>%{y}</b><br>Cantidad: %{x:,.0f}<extra></extra>'
                    )
                    if st.session_state.dark_mode:
                        fig_bar_h.update_layout(
                            height=320,
                            margin=dict(t=40, b=20, l=10, r=80),
                            showlegend=False,
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='#e2e8f0', family="Arial", size=11),
                            title=dict(font=dict(size=14, color='#e2e8f0')),
                            yaxis=dict(title="", tickfont=dict(size=10, color='#e2e8f0')),
                            xaxis=dict(title="", showgrid=True, gridcolor='#374151', tickfont=dict(color='#e2e8f0'))
                        )
                    else:
                        fig_bar_h.update_layout(
                            height=320,
                            margin=dict(t=40, b=20, l=10, r=80),
                            showlegend=False,
                            paper_bgcolor='#ffffff',
                            plot_bgcolor='#ffffff',
                            font=dict(color='#1f2937', family="Arial", size=11),
                            title=dict(font=dict(color='#1f2937', size=14)),
                            yaxis=dict(title="", tickfont=dict(size=10, color='#1f2937')),
                            xaxis=dict(title="", showgrid=True, gridcolor='#e5e7eb', tickfont=dict(color='#1f2937'))
                        )
                    st.plotly_chart(fig_bar_h, use_container_width=True, config={'displayModeBar': False})
            
            with col_chart2:
                if not df.empty and "alerta_stock" in df.columns:
                    df_stock_alerta = df.groupby("alerta_stock")["stock_1"].sum().reset_index()
                    df_stock_alerta.columns = ["Alerta", "Stock Total"]
                    df_stock_alerta["Color"] = df_stock_alerta["Alerta"].map(colores_alertas)
                    df_stock_alerta = df_stock_alerta.sort_values("Stock Total", ascending=True)
                    
                    fig_bar = px.bar(
                        df_stock_alerta,
                        y="Alerta",
                        x="Stock Total",
                        title="Stock por Tipo de Alerta",
                        color="Alerta",
                        color_discrete_map=colores_alertas,
                        text="Stock Total",
                        orientation='h'
                    )
                    fig_bar.update_traces(
                        texttemplate='%{text:,.0f}',
                        textposition='outside',
                        textfont_size=11,
                        marker=dict(line=dict(width=0), cornerradius=4),
                        hovertemplate='<b>%{y}</b><br>Stock: %{x:,.0f}<extra></extra>'
                    )
                    if st.session_state.dark_mode:
                        fig_bar.update_layout(
                            height=320,
                            margin=dict(t=40, b=20, l=10, r=80),
                            showlegend=False,
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='#e2e8f0', family="Arial", size=11),
                            title=dict(font=dict(size=14, color='#e2e8f0')),
                            yaxis=dict(title="", tickfont=dict(size=10, color='#e2e8f0')),
                            xaxis=dict(title="", showgrid=True, gridcolor='#374151', tickfont=dict(color='#e2e8f0'))
                        )
                    else:
                        fig_bar.update_layout(
                            height=320,
                            margin=dict(t=40, b=20, l=10, r=80),
                            showlegend=False,
                            paper_bgcolor='#ffffff',
                            plot_bgcolor='#ffffff',
                            font=dict(color='#1f2937', family="Arial", size=11),
                            title=dict(font=dict(color='#1f2937', size=14)),
                            yaxis=dict(title="", tickfont=dict(size=10, color='#1f2937')),
                            xaxis=dict(title="", showgrid=True, gridcolor='#e5e7eb', tickfont=dict(color='#1f2937'))
                        )
                    st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})
        
        st.divider()
        
        st.subheader("📋 Detalle de Artículos")
        
        if not df.empty:
            columnas_export = [
                "cod_articulo", "descripcion", "sucursal", "stock_1", 
                "total_venta", "venta_promedio_diaria", "venta_mensual_proyectada",
                "meses_stock", "alerta_stock"
            ]
            columnas_export_existentes = [c for c in columnas_export if c in df.columns]
            df_export = df[columnas_export_existentes].copy()
            df_export.columns = ["Código", "Descripción", "Sucursal", "Stock", 
                                 "Venta Total", "Vta. Diaria", "Vta. Mensual", 
                                 "Meses Stock", "Estado"][:len(columnas_export_existentes)]
            
            col_export, col_spacer = st.columns([1, 5])
            with col_export:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='Análisis Stock')
                excel_data = output.getvalue()
                st.download_button(
                    label="📊 Exportar Excel",
                    data=excel_data,
                    file_name="analisis_stock.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Descarga los datos filtrados en formato Excel (.xlsx)"
                )
            
            columnas_mostrar = [
                "cod_articulo", "descripcion", "sucursal", "stock_1", 
                "venta_promedio_diaria", "necesidad_periodo", "pedido_periodo", 
                "meses_stock", "alerta_display"
            ]
            
            columnas_existentes = [c for c in columnas_mostrar if c in df.columns]
            df_display = df[columnas_existentes].head(500).copy()
            
            sucursal_corto = {
                "LA TIJERA LUJAN": "LUJAN",
                "LA TIJERA MAIPU": "MAIPU", 
                "LA TIJERA MENDOZA": "MENDOZA",
                "LA TIJERA SAN JUAN": "SAN JUAN",
                "LA TIJERA SAN LUIS": "SAN LUIS",
                "LA TIJERA SAN RAFAEL": "SAN RAFAEL",
                "LA TIJERA SMARTIN": "SMARTIN",
                "LA TIJERA TUNUYAN": "TUNUYAN",
                "LA TIJERA MAYORISTA MENDOZA": "MAYORISTA MZA",
                "CRISA CENTRAL": "CDD",
                "CRISA 2": "CRISA 2"
            }
            if "sucursal" in df_display.columns:
                df_display["sucursal"] = df_display["sucursal"].replace(sucursal_corto)
            
            nombres_columnas = {
                "cod_articulo": "Código",
                "descripcion": "Descripción",
                "sucursal": "Sucursal",
                "stock_1": "Stock",
                "venta_promedio_diaria": "Vta. Diaria",
                "necesidad_periodo": f"Necesidad ({dias_proyeccion}d)",
                "pedido_periodo": f"Pedido ({dias_proyeccion}d)",
                "meses_stock": "Meses Stock",
                "alerta_display": "Estado"
            }
            
            df_display.columns = [nombres_columnas.get(c, c) for c in columnas_existentes]
            
            columnas_no_convertir = ["Código", "Descripción", "Sucursal", "Estado"]
            for col in df_display.columns:
                if col in columnas_no_convertir:
                    continue
                try:
                    if df_display[col].dtype in ['float64', 'float32', 'object']:
                        numeric_col = pd.to_numeric(df_display[col], errors='coerce')
                        if not numeric_col.isna().all():
                            df_display[col] = numeric_col.round(2)
                except:
                    pass
            
            def highlight_alerta(row):
                estado = str(row.get("Estado", ""))
                dark = st.session_state.dark_mode
                if "Quiebre de stock" in estado:
                    return ["background-color: #dc2626; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #fee2e2; color: #991b1b"] * len(row)
                elif "Stock de Seguridad" in estado:
                    return ["background-color: #2563eb; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #dbeafe; color: #1e40af"] * len(row)
                elif "Pto de Pedido" in estado:
                    return ["background-color: #0891b2; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #cffafe; color: #155e75"] * len(row)
                elif "OK" in estado:
                    return ["background-color: #16a34a; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #dcfce7; color: #166534"] * len(row)
                elif "Sobre stock" in estado:
                    return ["background-color: #d97706; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #fef3c7; color: #92400e"] * len(row)
                elif "Sin rotación" in estado:
                    return ["background-color: #6b7280; color: #ffffff; font-weight: 500"] * len(row) if dark else ["background-color: #f3f4f6; color: #374151"] * len(row)
                return ["color: #ffffff"] * len(row) if dark else ["color: #1f2937"] * len(row)
            
            df_styled = df_display.style.apply(highlight_alerta, axis=1)
            
            st.dataframe(
                df_styled,
                width="stretch",
                hide_index=True,
                height=400,
                column_config={
                    "Stock": st.column_config.NumberColumn(format="%.0f"),
                    "Vta. Diaria": st.column_config.NumberColumn(format="%.2f"),
                    f"Necesidad ({dias_proyeccion}d)": st.column_config.NumberColumn(format="%.0f"),
                    f"Pedido ({dias_proyeccion}d)": st.column_config.NumberColumn(format="%.0f"),
                    "Meses Stock": st.column_config.NumberColumn(format="%.1f"),
                }
            )
            
            if len(df) > 500:
                st.caption(f"Mostrando 500 de {len(df)} artículos. Usa los filtros para refinar.")
            else:
                st.caption(f"Mostrando {len(df_display)} artículos")
        else:
            st.info("No hay artículos que coincidan con los filtros seleccionados.")
        

with tab_distribucion:
    # Panel de Alertas para Jefe Logístico/Comercial
    prioridades_data = db.get_prioridades_distribucion()
    
    if prioridades_data:
        # Agrupar por prioridad
        alta = [p for p in prioridades_data if p['prioridad'] == 'ALTA']
        media = [p for p in prioridades_data if p['prioridad'] == 'MEDIA']
        
        # Estilos para tarjetas de prioridad
        st.markdown("""
        <style>
        .tarjeta-prioridad {
            border-radius: 10px;
            padding: 16px;
            margin-bottom: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .tarjeta-roja { background: #fee2e2; border-left: 5px solid #dc2626; }
        .tarjeta-amarilla { background: #fef9c3; border-left: 5px solid #ca8a04; }
        .tarjeta-verde { background: #dcfce7; border-left: 5px solid #16a34a; }
        .tarjeta-sucursal { font-size: 1.1rem; font-weight: 700; color: #1f2937; margin-bottom: 8px; }
        .tarjeta-dato { display: flex; justify-content: space-between; margin: 4px 0; }
        .tarjeta-label { color: #6b7280; font-size: 0.9rem; }
        .tarjeta-valor { font-weight: 600; color: #1f2937; font-size: 0.95rem; }
        </style>
        """, unsafe_allow_html=True)
        
        # Sección 1: Sugerido por Sistema (ROJO)
        st.markdown("##### 🔴 Sugerido por Sistema")
        st.caption("Distribución urgente basada en quiebres de stock detectados automáticamente")
        if alta:
            cols_alta = st.columns(min(len(alta), 4))
            for i, p in enumerate(alta[:4]):
                with cols_alta[i]:
                    top_cat = p['categorias'][0] if p['categorias'] else "-"
                    st.markdown(f"""
                    <div class="tarjeta-prioridad tarjeta-roja">
                        <div class="tarjeta-sucursal">{p['sucursal']}</div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Artículos</span>
                            <span class="tarjeta-valor">{p['articulos']:,}</span>
                        </div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Unidades</span>
                            <span class="tarjeta-valor">{p['unidades']:,.0f}</span>
                        </div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Categoría ppal.</span>
                            <span class="tarjeta-valor">{top_cat}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.button(f"Ver {p['sucursal']}", key=f"ver_alta_{i}", use_container_width=True):
                        st.session_state['filtro_sucursal_dist'] = p['sucursal']
                        st.rerun()
        else:
            st.markdown("""
            <div class="tarjeta-prioridad tarjeta-roja" style="text-align: center; padding: 15px; opacity: 0.7;">
                <div style="color: #dc2626;">✓ Sin distribuciones urgentes del sistema</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Sección 2: Sugerencia de Compras (AMARILLO)
        st.markdown("##### 🟡 Sugerencia de Compras")
        st.caption("Sucursales con stock en nivel de seguridad, planificar esta semana")
        if media:
            cols_media = st.columns(min(len(media), 4))
            for i, p in enumerate(media[:4]):
                with cols_media[i]:
                    top_cat = p['categorias'][0] if p['categorias'] else "-"
                    st.markdown(f"""
                    <div class="tarjeta-prioridad tarjeta-amarilla">
                        <div class="tarjeta-sucursal">{p['sucursal']}</div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Artículos</span>
                            <span class="tarjeta-valor">{p['articulos']:,}</span>
                        </div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Unidades</span>
                            <span class="tarjeta-valor">{p['unidades']:,.0f}</span>
                        </div>
                        <div class="tarjeta-dato">
                            <span class="tarjeta-label">Categoría ppal.</span>
                            <span class="tarjeta-valor">{top_cat}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.button(f"Ver {p['sucursal']}", key=f"ver_media_{i}", use_container_width=True):
                        st.session_state['filtro_sucursal_dist'] = p['sucursal']
                        st.rerun()
        else:
            st.markdown("""
            <div class="tarjeta-prioridad tarjeta-amarilla" style="text-align: center; padding: 15px; opacity: 0.7;">
                <div style="color: #ca8a04;">✓ Sin sugerencias de compras pendientes</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Sección 3: Solicitado por Directorio (VERDE)
        st.markdown("##### 🟢 Solicitado por el Directorio")
        st.caption("Distribuciones especiales indicadas por la gerencia (próximamente editable)")
        st.markdown("""
        <div class="tarjeta-prioridad tarjeta-verde" style="text-align: center; padding: 15px;">
            <div style="color: #16a34a;">Sin solicitudes pendientes del directorio</div>
            <div style="color: #6b7280; font-size: 0.85rem; margin-top: 4px;">Las solicitudes especiales aparecerán aquí</div>
        </div>
        """, unsafe_allow_html=True)
        
        if not alta and not media:
            st.success("✅ Todas las sucursales están abastecidas. No hay distribución urgente.")
    else:
        st.info("No hay datos de distribución disponibles.")
    
    st.divider()
    
    with st.expander("ℹ️ ¿Cómo funciona esta herramienta?", expanded=False):
        st.markdown("""
        **Objetivo:** Identificar qué artículos necesitan ser redistribuidos desde el Centro de Distribución (CDD) hacia las sucursales.
        
        **Lógica de cálculo:**
        - **Necesidad** = Venta promedio diaria × Días de proyección seleccionados
        - **Diferencia** = Necesidad - Stock actual (valores positivos = falta stock)
        - **Stock CDD** = Stock disponible en CRISA CENTRAL para distribuir
        
        **Interpretación de la matriz:**
        - Los números en cada celda indican cuántas unidades necesita cada sucursal
        - Solo se muestran artículos donde CDD tiene stock disponible y alguna sucursal necesita
        - **Total** = Suma de necesidades de todas las sucursales
        
        **Sugerencia de Distribución:** Calcula automáticamente cuánto enviar a cada sucursal, priorizando las que tienen mayor urgencia (quiebre de stock).
        """)
    
    # Detectar si viene de un click en las tarjetas de prioridad
    filtro_desde_tarjeta = st.session_state.get('filtro_categoria_dist')
    sucursal_desde_tarjeta = st.session_state.get('filtro_sucursal_dist')
    
    if filtro_desde_tarjeta:
        st.info(f"🎯 **Filtro activo:** {filtro_desde_tarjeta} en {sucursal_desde_tarjeta}")
        col_limpiar = st.columns([3, 1])
        with col_limpiar[1]:
            if st.button("✖️ Limpiar filtro", key="limpiar_filtro_tarjeta"):
                del st.session_state['filtro_categoria_dist']
                del st.session_state['filtro_sucursal_dist']
                st.rerun()
    
    col_f1, col_f2 = st.columns([1, 2])
    
    with col_f1:
        sucursales_dist = ["Todas"] + db.get_sucursales()
        # Usar sucursal desde tarjeta si existe
        default_suc_idx = 0
        if sucursal_desde_tarjeta:
            suc_full = f"LA TIJERA {sucursal_desde_tarjeta}"
            if suc_full in sucursales_dist:
                default_suc_idx = sucursales_dist.index(suc_full)
        sucursal_dist = st.selectbox("🏢 Sucursal", sucursales_dist, index=default_suc_idx, key="sucursal_dist")
    
    with col_f2:
        busqueda_dist = st.text_input(
            "🔍 Buscar artículo (usa * para colores)",
            key="busqueda_dist",
            placeholder="Ej: TA148L* busca 00-32"
        )
    
    col_fam, col_per = st.columns([2, 1])
    
    with col_fam:
        CATEGORIAS_POR_PREFIJO = {
            "BL": "BLANCO", "OT": "BLANCO", "BO": "BLANCO", "CO": "BLANCO", "SI": "BLANCO",
            "TD": "DECO", "MC": "DECO", "TM": "DECO",
            "TI": "INDUMENTARIA", "TA": "INDUMENTARIA", "TF": "INDUMENTARIA", "TV": "INDUMENTARIA", "PV": "INDUMENTARIA",
            "ME": "MERCERIA",
            "TC": "IMPULSO", "HS": "IMPULSO", "AR": "IMPULSO"
        }
        categorias_disponibles = ["BLANCO", "DECO", "INDUMENTARIA", "MERCERIA", "IMPULSO", "FIESTA", "INSTITUCIONAL", "COMODITIES", "MAQUINAS"]
        
        # Usar categoría desde tarjeta si existe
        default_cats = [filtro_desde_tarjeta] if filtro_desde_tarjeta and filtro_desde_tarjeta in categorias_disponibles else []
        
        categorias_seleccionadas = st.multiselect(
            "🏷️ Filtrar por categoría",
            options=categorias_disponibles,
            default=default_cats,
            placeholder="Todas las categorías",
            key=f"categorias_dist_{filtro_desde_tarjeta or 'none'}"
        )
        
        # Subcategorías basadas en categorías seleccionadas
        if categorias_seleccionadas:
            subcats_dist = []
            for cat in categorias_seleccionadas:
                subcats_dist.extend(db.get_subcategorias(cat))
            subcats_dist = sorted(list(set(subcats_dist)))
        else:
            subcats_dist = db.get_subcategorias()
        
        # Key dinámico para forzar actualización cuando cambia la categoría
        subcat_key = f"subcategorias_dist_{'_'.join(sorted(categorias_seleccionadas)) if categorias_seleccionadas else 'all'}"
        
        subcategorias_seleccionadas = st.multiselect(
            "📂 Filtrar por subcategoría",
            options=subcats_dist,
            default=[],
            placeholder="Todas las subcategorías",
            key=subcat_key
        )
    
    with col_per:
        dias_proyeccion = st.selectbox(
            "📅 Período de proyección",
            options=[15, 30, 60, 90],
            format_func=lambda x: f"{x} días próximos",
            key="dias_proyeccion_dist"
        )
    
    alertas_dist = ["OK", "Stock de Seguridad", "Quiebre de stock", "Pto de Pedido", "Sobre stock", "Sin rotación (sin stock)", "Sin rotación (con sobrestock)"]
    alertas_seleccionadas = st.multiselect(
        "🚨 Filtrar por punto de pedido",
        options=alertas_dist,
        default=[],
        placeholder="Todas las alertas",
        key="alertas_dist"
    )
    
    st.divider()
    
    alertas_param = alertas_seleccionadas if alertas_seleccionadas else None
    
    datos_matriz = db.get_matriz_distribucion(
        dias_proyeccion=dias_proyeccion,
        familias=None,
        alertas=alertas_param
    )
    
    if not datos_matriz:
        st.warning("⚠️ No hay datos para mostrar con los filtros seleccionados.")
    else:
        df_matriz = pd.DataFrame(datos_matriz)
        
        def obtener_categoria(cod_articulo):
            if not cod_articulo:
                return None
            prefijo = cod_articulo[:2].upper()
            return CATEGORIAS_POR_PREFIJO.get(prefijo, None)
        
        df_matriz["categoria"] = df_matriz["cod_articulo"].apply(obtener_categoria)
        
        if categorias_seleccionadas:
            df_matriz = df_matriz[df_matriz["categoria"].isin(categorias_seleccionadas)]  # type: ignore
        
        # Filtrar por subcategorías seleccionadas
        if subcategorias_seleccionadas:
            articulos_subcat = db.get_articulos_por_categoria([], subcategorias_seleccionadas)
            if articulos_subcat:
                df_matriz = df_matriz[df_matriz["cod_articulo"].isin(articulos_subcat)]  # type: ignore
        
        if busqueda_dist and busqueda_dist.strip():
            terminos_dist = [t.strip().upper() for t in busqueda_dist.split(",") if t.strip()]
            if terminos_dist:
                mask_dist = pd.Series([False] * len(df_matriz))
                for termino in terminos_dist:
                    if termino.endswith("*"):
                        codigo_base = termino[:-1]
                        mask_base = df_matriz["cod_articulo"].astype(str).str.upper().str.startswith(codigo_base)  # type: ignore
                        mask_dist = mask_dist | mask_base
                    else:
                        mask_cod = df_matriz["cod_articulo"].astype(str).str.upper().str.contains(termino, na=False)  # type: ignore
                        mask_desc = df_matriz["descripcion"].astype(str).str.upper().str.contains(termino, na=False) if "descripcion" in df_matriz.columns else pd.Series([False] * len(df_matriz))  # type: ignore
                        mask_dist = mask_dist | mask_cod | mask_desc
                df_matriz = df_matriz[mask_dist]
        
        if sucursal_dist != "Todas":
            df_matriz = df_matriz[df_matriz["sucursal"] == sucursal_dist]
        
        st.markdown("### 📦 Necesidad de Distribución")
        st.caption(f"📊 Matriz de necesidades para los próximos {dias_proyeccion} días")
        with st.expander("ℹ️ ¿Cómo leer esta matriz?", expanded=False):
            st.markdown(f"""
| Columna | Descripción |
|---------|-------------|
| **Stock CDD** | Unidades disponibles en CRISA CENTRAL para distribuir |
| **Sucursales** | Unidades que necesita cada sucursal para cubrir {dias_proyeccion} días de venta |
| **Cant. Total** | Suma de unidades necesarias en todas las sucursales |

⚠️ *Valores expresados en UNIDADES (no en pesos). Solo muestra artículos con stock en CDD y demanda en sucursales.*
""")
        
        sucursales_unicas = df_matriz["sucursal"].unique().tolist()  # type: ignore
        articulos_unicos = df_matriz["cod_articulo"].unique().tolist()  # type: ignore
        
        if len(sucursales_unicas) > 1 and len(articulos_unicos) > 0:
            pivot_necesidad = df_matriz.pivot_table(  # type: ignore
                index="cod_articulo",
                columns="sucursal",
                values="diferencia",
                aggfunc="sum",
                fill_value=0
            )
            
            pivot_stock_central = df_matriz[df_matriz["sucursal"] == "CRISA CENTRAL"].groupby("cod_articulo")["stock_1"].sum()  # type: ignore
            
            pivot_necesidad = pivot_necesidad.reset_index()
            
            if "CRISA CENTRAL" in pivot_necesidad.columns:
                pivot_necesidad = pivot_necesidad.drop(columns=["CRISA CENTRAL"])
            
            pivot_necesidad["Stock CDD"] = pivot_necesidad["cod_articulo"].map(pivot_stock_central).fillna(0)  # type: ignore
            
            cols = ["cod_articulo", "Stock CDD"]
            sucursales_excluir = ["LA TIJERA MAYORISTA MENDOZA"]
            otras_sucursales = [c for c in pivot_necesidad.columns if c not in cols and c != "Total" and c not in sucursales_excluir]
            otras_sucursales = sorted(otras_sucursales)
            pivot_necesidad = pivot_necesidad[cols + otras_sucursales]
            
            for col in otras_sucursales:
                pivot_necesidad[col] = pivot_necesidad[col].clip(lower=0)  # type: ignore
            
            pivot_necesidad["Cant. Total"] = pivot_necesidad[otras_sucursales].sum(axis=1)
            
            pivot_necesidad = pivot_necesidad[
                (pivot_necesidad["Stock CDD"] > 0) & 
                (pivot_necesidad["Cant. Total"] > 0)
            ]
            
            pivot_necesidad = pivot_necesidad.sort_values("Stock CDD", ascending=False)  # type: ignore
            
            nombre_corto = {
                "LA TIJERA LUJAN": "LUJAN",
                "LA TIJERA MAIPU": "MAIPU", 
                "LA TIJERA MENDOZA": "MENDOZA",
                "LA TIJERA SAN JUAN": "SAN JUAN",
                "LA TIJERA SAN LUIS": "SAN LUIS",
                "LA TIJERA SAN RAFAEL": "SAN RAFAEL",
                "LA TIJERA SMARTIN": "SMARTIN",
                "LA TIJERA TUNUYAN": "TUNUYAN",
                "CRISA 2": "CRISA 2"
            }
            pivot_necesidad = pivot_necesidad.rename(columns=nombre_corto)
            
            pivot_limited = pivot_necesidad.head(100)
            
            st.dataframe(
                pivot_limited,
                width="stretch",
                hide_index=True,
                height=min(400, 50 + min(len(articulos_unicos), 100) * 35)
            )
            
            col_info, col_download = st.columns([3, 1])
            with col_info:
                if len(articulos_unicos) > 100:
                    st.caption(f"Mostrando 100 de {len(articulos_unicos)} artículos. Usa los filtros para refinar.")
            with col_download:
                from io import BytesIO
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:  # type: ignore
                    pivot_necesidad.to_excel(writer, sheet_name='Necesidad', index=False)
                output.seek(0)
                st.download_button(
                    label="📥 Descargar Excel",
                    data=output,
                    file_name=f"necesidad_distribucion_{dias_proyeccion}dias.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_necesidad"
                )
            
            col_t1, col_t2, col_t3 = st.columns(3)
            with col_t1:
                total_necesidad = float(df_matriz[df_matriz["diferencia"] > 0]["diferencia"].sum())
                st.metric("Necesidad Total", f"{total_necesidad:,.0f}")
            with col_t2:
                total_exceso = float(df_matriz[df_matriz["diferencia"] < 0]["diferencia"].sum())
                st.metric("Exceso Total", f"{abs(total_exceso):,.0f}")
            with col_t3:
                st.metric("Artículos", len(articulos_unicos))
        else:
            st.dataframe(
                df_matriz[["cod_articulo", "descripcion", "sucursal", "stock_1", "necesidad_periodo", "diferencia", "alerta_stock"]],
                width="stretch",
                hide_index=True
            )
        
        st.divider()
        
        st.markdown("### Sugerencia de Distribución")
        
        # Radio buttons de período
        periodo_sug = st.radio(
            "Período de proyección:",
            ["15 días", "30 días", "60 días"],
            horizontal=True,
            key="radio_periodo_sugerencia"
        )
        dias_sug = int(periodo_sug.split()[0])
        
        st.caption(f"💡 **Recomendación automática para cubrir {dias_sug} días de venta.** Prioriza sucursales en quiebre. **Sug. Distribuir** = cantidad a enviar. **Pedido** = unidades a comprar si CDD no alcanza.")
        
        sugerencias = db.get_sugerencia_distribucion(
            dias_proyeccion=dias_sug,
            familias=None
        )
        
        # Obtener TODAS las sucursales del sistema (excluir CDD)
        todas_sucursales = db.get_sucursales()
        nombres_cortos = {}
        for suc in todas_sucursales:
            # Omitir CRISA CENTRAL (es el centro de distribución)
            if suc == "CRISA CENTRAL":
                continue
            elif suc.startswith("LA TIJERA "):
                nombre_limpio = suc.replace("LA TIJERA ", "").strip()
            elif suc == "CRISA 2":
                nombre_limpio = "CRISA 2"
            else:
                nombre_limpio = suc
            nombres_cortos[nombre_limpio] = suc
        
        # Radio buttons para filtrar sucursales (todas las sucursales)
        opciones_radio = ["Todas"] + sorted(nombres_cortos.keys())
        filtro_suc_radio = st.radio(
            "Filtrar por sucursal:",
            opciones_radio,
            horizontal=True,
            key="radio_sucursal_sugerencia"
        )
        
        if not sugerencias:
            st.info("No hay sugerencias de distribución. Todos los artículos tienen stock suficiente.")
        else:
            df_sugerencias = pd.DataFrame(sugerencias)
            
            # Convertir nombre corto a nombre completo para filtrar
            if filtro_suc_radio != "Todas":
                sucursal_completa = nombres_cortos.get(filtro_suc_radio, filtro_suc_radio)
                df_sugerencias = df_sugerencias[df_sugerencias["sucursal"] == sucursal_completa]
                if df_sugerencias.empty:
                    st.success(f"✅ {filtro_suc_radio} no tiene sugerencias pendientes - Stock OK")
            elif sucursal_dist != "Todas":
                df_sugerencias = df_sugerencias[df_sugerencias["sucursal"] == sucursal_dist]
            
            if not df_sugerencias.empty:
                cols_to_show = ["sucursal", "cod_articulo", "stock_cdd", "stock_sucursal", 
                    "necesidad", "sugerencia_distribuir", "pedido", "meses_stock", "alerta_stock"]
                cols_available = [c for c in cols_to_show if c in df_sugerencias.columns]
                df_sug_display = df_sugerencias[cols_available].copy()
                
                def acortar_suc(nombre):
                    if nombre in ["CRISA 2", "CRISA CENTRAL"]:
                        return "CDD" if nombre == "CRISA CENTRAL" else nombre
                    return nombre.replace("LA TIJERA ", "")
                df_sug_display["sucursal"] = df_sug_display["sucursal"].apply(acortar_suc)  # type: ignore
                
                df_sug_display["alerta_icon"] = df_sug_display["alerta_stock"].apply(get_alerta_color)  # type: ignore
                df_sug_display["alerta_display"] = df_sug_display["alerta_icon"] + " " + df_sug_display["alerta_stock"]
                
                rename_map = {
                    "sucursal": "Sucursal",
                    "cod_articulo": "Cód. Artículo",
                    "stock_cdd": "Stock C.D.D",
                    "stock_sucursal": "Stock Sucursal",
                    "necesidad": "Necesidad",
                    "sugerencia_distribuir": "Sug. Distribuir",
                    "pedido": "Pedido",
                    "meses_stock": "Meses de stock",
                    "alerta_display": "Alerta de Sucursales"
                }
                df_sug_display = df_sug_display.rename(columns=rename_map)  # type: ignore
                
                cols_to_drop = [c for c in ["alerta_stock", "alerta_icon"] if c in df_sug_display.columns]
                df_sug_display = df_sug_display.drop(columns=cols_to_drop)
                
                st.dataframe(
                    df_sug_display,
                    width="stretch",
                    hide_index=True,
                    height=300,
                    column_config={
                        "Stock C.D.D": st.column_config.NumberColumn(format="%.1f"),
                        "Stock Sucursal": st.column_config.NumberColumn(format="%.0f"),
                        "Necesidad": st.column_config.NumberColumn(format="%.0f"),
                        "Sug. Distribuir": st.column_config.NumberColumn(format="%.0f"),
                        "Pedido": st.column_config.NumberColumn(format="%.0f"),
                        "Meses de stock": st.column_config.NumberColumn(format="%.1f"),
                    }
                )
                
                col_sug_space, col_sug_download = st.columns([3, 1])
                with col_sug_download:
                    from io import BytesIO
                    output_sug = BytesIO()
                    with pd.ExcelWriter(output_sug, engine='openpyxl') as writer:  # type: ignore
                        df_sug_display.to_excel(writer, sheet_name='Sugerencias', index=False)
                    output_sug.seek(0)
                    st.download_button(
                        label="📥 Descargar Excel",
                        data=output_sug,
                        file_name=f"sugerencia_distribucion_{dias_proyeccion}dias.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_sugerencias"
                    )
                
                FRANQUICIAS = ["LUJAN", "MAIPU", "SAN RAFAEL", "SMARTIN", "TUNUYAN"]
                
                precios_dict = {}
                all_precios = db.get_precios()
                for p in all_precios:
                    cod = str(p.get("cod_articulo", "")).strip()
                    lista = str(p.get("nro_lista", "")).strip()
                    precio = float(p.get("precio", 0) or 0)
                    if cod not in precios_dict:
                        precios_dict[cod] = {}
                    precios_dict[cod][lista] = precio
                
                def get_precio_por_sucursal(row):
                    cod = str(row.get("Cód. Artículo", "")).strip()
                    suc = row.get("Sucursal", "")
                    lista = "102" if suc in FRANQUICIAS else "2"
                    return precios_dict.get(cod, {}).get(lista, 0)
                
                df_sug_display["Precio"] = df_sug_display.apply(get_precio_por_sucursal, axis=1).astype(float)
                df_sug_display["Sug. Distribuir"] = df_sug_display["Sug. Distribuir"].astype(float)
                df_sug_display["Valor"] = df_sug_display["Sug. Distribuir"] * df_sug_display["Precio"]
                
                df_propias = df_sug_display[~df_sug_display["Sucursal"].isin(FRANQUICIAS)]
                df_franquicias = df_sug_display[df_sug_display["Sucursal"].isin(FRANQUICIAS)]
                
                valor_propias = float(df_propias["Valor"].sum())
                valor_franquicias = float(df_franquicias["Valor"].sum())
                lineas_propias = len(df_propias[df_propias["Sug. Distribuir"] > 0])
                lineas_franquicias = len(df_franquicias[df_franquicias["Sug. Distribuir"] > 0])
                
                col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                with col_s1:
                    valor_propias_fmt = f"${valor_propias:,.0f}".replace(",", ".")
                    st.metric("💼 $ Propias", valor_propias_fmt, help="Valor total en $ a distribuir hacia sucursales propias (Lista 2)")
                with col_s2:
                    valor_franquicias_fmt = f"${valor_franquicias:,.0f}".replace(",", ".")
                    st.metric("🏪 $ Franquicias", valor_franquicias_fmt, help="Valor total en $ a distribuir hacia franquicias (Lista 102)")
                with col_s3:
                    st.metric("📦 Art. Propias", lineas_propias, help="Cantidad de artículos a distribuir hacia sucursales propias")
                with col_s4:
                    st.metric("📦 Art. Franquicias", lineas_franquicias, help="Cantidad de artículos a distribuir hacia franquicias")
            else:
                st.info("No hay sugerencias para la sucursal seleccionada.")

# TAB KRALJIC OCULTO TEMPORALMENTE
if False:  # with tab_kraljic:
    st.markdown("### Matriz de Kraljic - Portafolio de Compras")
    st.markdown("""
    La Matriz de Kraljic clasifica los artículos según su **impacto financiero** (valor de ventas) 
    y **riesgo de suministro** (meses de stock), definiendo la estrategia óptima para cada uno.
    """)
    
    col_k1, col_k2, col_k3, col_k4 = st.columns([2, 2, 3, 2])
    
    with col_k1:
        sucursales_k = ["Todas"] + db.get_sucursales()
        sucursal_kraljic = st.selectbox(
            "🏢 Sucursal", 
            sucursales_k, 
            key="sucursal_kraljic",
            format_func=lambda x: x if x in ["Todas", "CRISA 2"] else ("CDD" if x == "CRISA CENTRAL" else x.replace("LA TIJERA ", ""))
        )
    
    with col_k2:
        categorias_k = db.get_categorias()
        categoria_kraljic = st.multiselect("📁 Categoría", categorias_k, key="categoria_kraljic", placeholder="Todas las categorías...")
    
    with col_k3:
        busqueda_kraljic = st.text_input(
            "🔍 Buscar (usa * para colores)",
            key="busqueda_kraljic",
            placeholder="Ej: TA148L* busca 00-32"
        )
    
    with col_k4:
        st.markdown("")
        st.markdown("**Cuadrantes:**")
        st.markdown("🟢🔵🟡🔴")
    
    st.divider()
    
    sucursal_k_param = sucursal_kraljic if sucursal_kraljic != "Todas" else None
    metricas_k = db.get_metricas(sucursal_k_param, None)
    
    if metricas_k:
        df_k = pd.DataFrame(metricas_k)
        
        if categoria_kraljic:
            articulos_cat = db.get_articulos_por_categoria(categoria_kraljic, None)
            if articulos_cat:
                df_k = df_k[df_k["cod_articulo"].isin(articulos_cat)]
        
        if busqueda_kraljic and busqueda_kraljic.strip():
            terminos = [t.strip().upper() for t in busqueda_kraljic.split(",") if t.strip()]
            if terminos:
                mask = pd.Series([False] * len(df_k))
                for termino in terminos:
                    if termino.endswith("*"):
                        codigo_base = termino[:-1]
                        codigos_colores = [f"{codigo_base}{str(i).zfill(2)}" for i in range(0, 33)]
                        mask_cod = df_k["cod_articulo"].astype(str).str.upper().isin(codigos_colores)
                        mask_base = df_k["cod_articulo"].astype(str).str.upper().str.startswith(codigo_base)
                        mask = mask | mask_cod | mask_base
                    else:
                        mask_cod = df_k["cod_articulo"].astype(str).str.upper().str.contains(termino, na=False)
                        mask_desc = df_k["descripcion"].astype(str).str.upper().str.contains(termino, na=False)
                        mask_fam = df_k["familia"].astype(str).str.upper().str.contains(termino, na=False) if "familia" in df_k.columns else pd.Series([False] * len(df_k))
                        mask_desc_fam = df_k["desc_familia"].astype(str).str.upper().str.contains(termino, na=False) if "desc_familia" in df_k.columns else pd.Series([False] * len(df_k))
                        mask = mask | mask_cod | mask_desc | mask_fam | mask_desc_fam
                df_k = df_k[mask]
        
        df_k = df_k[df_k["stock_1"] > 0]
        
        for col in ["stock_1", "total_venta", "venta_mensual_proyectada", "meses_stock"]:
            if col in df_k.columns:
                df_k[col] = pd.to_numeric(df_k[col], errors='coerce').fillna(0)
        
        df_k["valor_stock"] = df_k["stock_1"] * df_k.get("precio", 1)
        
        if not df_k.empty and "total_venta" in df_k.columns and "meses_stock" in df_k.columns:
            venta_mediana = df_k["total_venta"].median()
            meses_mediana = df_k["meses_stock"].median()
            
            if venta_mediana == 0:
                venta_mediana = df_k["total_venta"].mean()
            if meses_mediana == 0:
                meses_mediana = 3
            
            def clasificar_kraljic(row):
                venta = row["total_venta"]
                meses = row["meses_stock"]
                
                alto_impacto = venta >= venta_mediana
                alto_riesgo = meses <= meses_mediana
                
                if alto_impacto and not alto_riesgo:
                    return "Apalancados"
                elif alto_impacto and alto_riesgo:
                    return "Estratégicos"
                elif not alto_impacto and not alto_riesgo:
                    return "Rutinarios"
                else:
                    return "Cuello de botella"
            
            df_k["kraljic"] = df_k.apply(clasificar_kraljic, axis=1)
            
            max_meses = df_k["meses_stock"].max() if df_k["meses_stock"].max() > 0 else 12
            max_venta = df_k["total_venta"].max() if df_k["total_venta"].max() > 0 else 1000
            
            df_k["riesgo_norm"] = 100 - (df_k["meses_stock"] / max_meses * 100).clip(0, 100)
            df_k["impacto_norm"] = (df_k["total_venta"] / max_venta * 100).clip(0, 100)
            
            colores_kraljic = {
                "Rutinarios": "#22c55e",
                "Apalancados": "#3b82f6",
                "Cuello de botella": "#eab308",
                "Estratégicos": "#ef4444"
            }
            
            fig_kraljic = px.scatter(
                df_k,
                x="riesgo_norm",
                y="impacto_norm",
                color="kraljic",
                size="stock_1",
                hover_name="descripcion",
                hover_data={
                    "cod_articulo": True,
                    "sucursal": True,
                    "stock_1": ":.0f",
                    "total_venta": ":.0f",
                    "meses_stock": ":.1f",
                    "riesgo_norm": False,
                    "impacto_norm": False
                },
                color_discrete_map=colores_kraljic,
                category_orders={"kraljic": ["Apalancados", "Estratégicos", "Rutinarios", "Cuello de botella"]}
            )
            
            fig_kraljic.add_shape(type="rect", x0=0, y0=50, x1=50, y1=100,
                                  fillcolor="rgba(59, 130, 246, 0.15)", line=dict(width=2, color="#3b82f6"), layer="below")
            fig_kraljic.add_shape(type="rect", x0=50, y0=50, x1=100, y1=100,
                                  fillcolor="rgba(239, 68, 68, 0.15)", line=dict(width=2, color="#ef4444"), layer="below")
            fig_kraljic.add_shape(type="rect", x0=0, y0=0, x1=50, y1=50,
                                  fillcolor="rgba(34, 197, 94, 0.15)", line=dict(width=2, color="#22c55e"), layer="below")
            fig_kraljic.add_shape(type="rect", x0=50, y0=0, x1=100, y1=50,
                                  fillcolor="rgba(234, 179, 8, 0.15)", line=dict(width=2, color="#eab308"), layer="below")
            
            fig_kraljic.add_annotation(x=25, y=90, text="<b>APALANCADOS</b><br><i>Negociar precios</i>",
                                       showarrow=False, font=dict(size=12, color="#1e40af"), align="center")
            fig_kraljic.add_annotation(x=75, y=90, text="<b>ESTRATÉGICOS</b><br><i>Alianzas clave</i>",
                                       showarrow=False, font=dict(size=12, color="#991b1b"), align="center")
            fig_kraljic.add_annotation(x=25, y=10, text="<b>RUTINARIOS</b><br><i>Simplificar</i>",
                                       showarrow=False, font=dict(size=12, color="#166534"), align="center")
            fig_kraljic.add_annotation(x=75, y=10, text="<b>CUELLO DE BOTELLA</b><br><i>Asegurar stock</i>",
                                       showarrow=False, font=dict(size=12, color="#854d0e"), align="center")
            
            fig_kraljic.add_hline(y=50, line_dash="dash", line_color="#6b7280", line_width=1)
            fig_kraljic.add_vline(x=50, line_dash="dash", line_color="#6b7280", line_width=1)
            
            if st.session_state.dark_mode:
                fig_kraljic.update_layout(
                    height=550,
                    title=dict(text="Matriz de Kraljic - Portafolio de Compras", font=dict(size=16, color="#e2e8f0")),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(26,26,46,0.5)',
                    font=dict(color='#e2e8f0'),
                    xaxis=dict(title="← Bajo Riesgo | Alto Riesgo →", range=[-5, 105], showgrid=False, zeroline=False),
                    yaxis=dict(title="← Bajo Impacto | Alto Impacto →", range=[-5, 105], showgrid=False, zeroline=False),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=11)),
                    margin=dict(t=80, b=60, l=60, r=40)
                )
            else:
                fig_kraljic.update_layout(
                    height=550,
                    title=dict(text="Matriz de Kraljic - Portafolio de Compras", font=dict(size=16, color="#1f2937")),
                    paper_bgcolor='#ffffff',
                    plot_bgcolor='#fafafa',
                    font=dict(color='#1f2937'),
                    xaxis=dict(title="← Bajo Riesgo | Alto Riesgo →", range=[-5, 105], showgrid=False, zeroline=False),
                    yaxis=dict(title="← Bajo Impacto | Alto Impacto →", range=[-5, 105], showgrid=False, zeroline=False),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=11)),
                    margin=dict(t=80, b=60, l=60, r=40)
                )
            
            fig_kraljic.update_traces(marker=dict(line=dict(width=1, color='white')))
            
            st.plotly_chart(fig_kraljic, use_container_width=True)
            
            st.divider()
            
            col_res1, col_res2, col_res3, col_res4 = st.columns(4)
            
            conteo_kraljic = df_k["kraljic"].value_counts()
            
            with col_res1:
                count_rut = conteo_kraljic.get("Rutinarios", 0)
                st.metric("🟢 Rutinarios", f"{count_rut:,}", help="Bajo impacto, bajo riesgo. Simplificar procesos.")
            with col_res2:
                count_apa = conteo_kraljic.get("Apalancados", 0)
                st.metric("🔵 Apalancados", f"{count_apa:,}", help="Alto impacto, bajo riesgo. Negociar precios.")
            with col_res3:
                count_cue = conteo_kraljic.get("Cuello de botella", 0)
                st.metric("🟡 Cuello Botella", f"{count_cue:,}", help="Bajo impacto, alto riesgo. Asegurar suministro.")
            with col_res4:
                count_est = conteo_kraljic.get("Estratégicos", 0)
                st.metric("🔴 Estratégicos", f"{count_est:,}", help="Alto impacto, alto riesgo. Alianzas estratégicas.")
            
            st.divider()
            
            with st.expander("📋 Detalle por Categoría Kraljic", expanded=False):
                kraljic_sel = st.selectbox("Seleccionar categoría:", 
                                           ["Todos", "Estratégicos", "Apalancados", "Cuello de botella", "Rutinarios"],
                                           key="kraljic_detalle")
                
                if kraljic_sel != "Todos":
                    df_detalle = df_k[df_k["kraljic"] == kraljic_sel]
                else:
                    df_detalle = df_k
                
                df_detalle_show = df_detalle[["cod_articulo", "descripcion", "sucursal", "stock_1", "total_venta", "meses_stock", "kraljic"]].head(100)
                df_detalle_show.columns = ["Código", "Descripción", "Sucursal", "Stock", "Venta Total", "Meses Stock", "Categoría"]
                
                st.dataframe(df_detalle_show, width="stretch", hide_index=True)
    else:
        st.warning("No hay datos disponibles. Sincroniza primero desde el bridge.")

with st.sidebar:
    st.title("ℹ️ Información")
    
    with st.expander("🎯 **TOUR GUIADO DEL SISTEMA**", expanded=False):
        st.markdown("""
### 📊 ¿Qué hace este sistema?
Transforma los datos del ERP Tango en **decisiones de reposición** claras y accionables.

---

### 🖥️ MENÚ RESUMEN
**Objetivo:** Ver el estado de stock de todas las sucursales de un vistazo.

| Elemento | Descripción |
|----------|-------------|
| 🔴 **Tarjeta Roja** | Sucursal crítica con mucho faltante |
| 🟡 **Tarjeta Amarilla** | Sucursal con algunos faltantes |
| 🟢 **Tarjeta Verde** | Sucursal con stock OK |
| 💰 **Valor Total** | Inversión necesaria para reponer (Lista 2 = precio público) |
| 📦 **Art. Críticos** | Cantidad de artículos en quiebre + seguridad |

**Alertas dentro de cada tarjeta:**
- 🚨 **Quiebre** → Sin stock, venta perdida
- ⚠️ **Seguridad** → Stock para 1-2 meses
- 📋 **Pedido** → Necesita reposición pronto
- 📦 **Sobrestock** → Exceso >6 meses
- 💤 **Sin rotación** → Sin ventas
- ✅ **OK** → Stock normal (2-6 meses)

---

### 🔄 MENÚ DISTRIBUCIÓN
**Objetivo:** Identificar qué artículos redistribuir desde el Centro de Distribución (CDD).

| Filtro | Uso |
|--------|-----|
| 🏷️ **Categoría** | Filtrar por familia (BLANCO, DECO, etc.) |
| 📂 **Subcategoría** | Filtrar específico (BAÑO, SALA, etc.) |
| 📅 **Período** | Proyección a 15/30/60/90 días |

**Matriz de necesidades:**
- Cada celda = unidades que necesita esa sucursal
- **Stock CDD** = disponible en Central para enviar
- **Total** = suma de necesidades de todas las sucursales

---

### 📧 COMUNICACIÓN EMAIL
3 tipos de reportes automáticos:
1. **Resumen General** → Semáforo de todas las sucursales
2. **Alerta Sucursal** → Detalle urgente específico
3. **Resumen Comercial** → Montos para el área comercial

---

### 📱 WHATSAPP (Twilio)
- Alertas instantáneas al celular
- Notificaciones de quiebre de stock
- Reportes resumidos por sucursal

---

### 📊 ORIGEN DE DATOS
Los datos provienen del **ERP Tango** (SQL Server) mediante sincronización automática cada 5 minutos.
        """)
        
        st.markdown("---")
        if st.button("🚀 Iniciar Tour Interactivo", key="btn_iniciar_tour", use_container_width=True, type="primary"):
            st.session_state.show_tour = True
            st.session_state.tour_step = 0
            st.rerun()
    
    st.divider()
    
    st.markdown("### 🚨 Alertas de Stock")
    st.markdown("""
    | Icono | Estado | Meses Stock |
    |-------|--------|-------------|
    | 🟢 | Normal | 2 - 6 meses |
    | 🔵 | Seguridad | 1 - 2 meses |
    | 🟡 | Sobrestock | > 6 meses |
    | 🟠 | Sin rotación | Sin ventas |
    | 🔴 | Quiebre | < 1 mes |
    """)
    
    st.divider()
    
    st.markdown("### 📐 Fórmulas")
    st.markdown("""
    - **Vta. Diaria** = Total ventas / Días período
    - **Vta. Mensual** = Vta. Diaria × 30
    - **Meses Stock** = Stock / Vta. Mensual
    - **Necesidad** = Vta. Diaria × Días proyección
    - **Diferencia** = Necesidad - Stock actual
    """)
    
    st.divider()
    
    st.markdown("### 🔄 Última Sincronización")
    
    if last_sync:
        ts = last_sync.get('timestamp')
        if ts:
            st.caption(f"📅 {str(ts)[:19]}")
        
        col_sync1, col_sync2 = st.columns(2)
        with col_sync1:
            st.metric("Saldo", f"{last_sync.get('registros_saldo', 0):,}")
        with col_sync2:
            st.metric("Ventas", f"{last_sync.get('registros_ventas', 0):,}")
        
        status = last_sync.get('status', 'N/A')
        if status == 'ok':
            st.success("Estado: OK")
        else:
            st.error(f"Estado: {status}")
    else:
        st.warning("Sin sincronización")

# ============== PESTAÑA COSTOS ==============
with tab_costos:
    st.markdown("## 💰 Gestión de Costos de Reposición")
    
    col_upload, col_resumen = st.columns([2, 1])
    
    with col_upload:
        st.markdown("### 📤 Cargar Costos")
        st.markdown("""
        Subí un archivo **Excel** o **CSV** con las siguientes columnas:
        - `cod_articulo`: Código del artículo (obligatorio)
        - `descripcion`: Descripción del artículo
        - `costo_reposicion`: Costo unitario de reposición
        - `moneda`: Moneda (ARS por defecto)
        """)
        
        uploaded_file = st.file_uploader(
            "Seleccionar archivo",
            type=['xlsx', 'xls', 'csv'],
            key="costos_uploader"
        )
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df_costos = pd.read_csv(uploaded_file)
                else:
                    df_costos = pd.read_excel(uploaded_file)
                
                st.success(f"Archivo cargado: {len(df_costos)} registros")
                
                required_cols = ['cod_articulo', 'costo_reposicion']
                missing_cols = [c for c in required_cols if c not in df_costos.columns]
                
                if missing_cols:
                    st.error(f"Faltan columnas obligatorias: {', '.join(missing_cols)}")
                else:
                    st.dataframe(
                        df_costos.head(10),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    if st.button("📥 Importar Costos", type="primary", key="btn_import_costos"):
                        costos_list = []
                        for _, row in df_costos.iterrows():
                            costos_list.append({
                                'cod_articulo': str(row.get('cod_articulo', '')),
                                'descripcion': str(row.get('descripcion', '')),
                                'costo_reposicion': float(row.get('costo_reposicion', 0) or 0),
                                'moneda': str(row.get('moneda', 'ARS'))
                            })
                        
                        try:
                            count = db.upsert_costos(costos_list)
                            st.success(f"✅ Se importaron {count} costos")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al importar: {str(e)}")
            except Exception as e:
                st.error(f"Error al leer archivo: {str(e)}")
    
    with col_resumen:
        st.markdown("### 📊 Resumen")
        try:
            all_costos = db.get_all_costos()
            total_costos = len(all_costos)
            
            st.metric("Artículos con Costo", f"{total_costos:,}")
            
            if total_costos > 0:
                if st.button("🗑️ Eliminar Todos", type="secondary", key="btn_delete_costos"):
                    try:
                        db.delete_all_costos()
                        st.success("Costos eliminados")
                        st.rerun()
                    except:
                        st.error("Error al eliminar")
        except:
            st.metric("Artículos con Costo", "0")
    
    st.divider()
    
    st.markdown("### 💵 Valor de Stock y Reposición por Sucursal")
    
    try:
        resumen_data = db.get_resumen_costos_por_sucursal()
        
        if resumen_data:
            df_resumen = pd.DataFrame(resumen_data)
            
            df_resumen['sucursal'] = df_resumen['sucursal'].str.replace('LA TIJERA ', '')
            
            def format_money(val):
                if pd.isna(val) or val == 0:
                    return "-"
                return f"${val:,.0f}"
            
            st.dataframe(
                df_resumen.style.format({
                    'total_unidades': '{:,.0f}',
                    'valor_stock_total': format_money,
                    'valor_reposicion_urgente': format_money,
                    'quiebres': '{:,.0f}',
                    'seguridad': '{:,.0f}',
                    'sobrestock': '{:,.0f}'
                }),
                column_config={
                    'sucursal': st.column_config.TextColumn('Sucursal', width='medium'),
                    'total_articulos': st.column_config.NumberColumn('Artículos', format='%d'),
                    'total_unidades': st.column_config.NumberColumn('Unidades', format='%.0f'),
                    'valor_stock_total': st.column_config.TextColumn('Valor Stock'),
                    'valor_reposicion_urgente': st.column_config.TextColumn('Reposición Urgente'),
                    'quiebres': st.column_config.NumberColumn('🔴', format='%d'),
                    'seguridad': st.column_config.NumberColumn('🟡', format='%d'),
                    'sobrestock': st.column_config.NumberColumn('🟣', format='%d')
                },
                use_container_width=True,
                hide_index=True
            )
            
            col_tot1, col_tot2, col_tot3 = st.columns(3)
            with col_tot1:
                total_valor_stock = sum(r.get('valor_stock_total', 0) or 0 for r in resumen_data)
                st.metric("💵 Valor Total Stock", f"${total_valor_stock:,.0f}")
            with col_tot2:
                total_reposicion = sum(r.get('valor_reposicion_urgente', 0) or 0 for r in resumen_data)
                st.metric("🚨 Reposición Urgente", f"${total_reposicion:,.0f}")
            with col_tot3:
                total_quiebres = sum(r.get('quiebres', 0) or 0 for r in resumen_data)
                st.metric("🔴 Total Quiebres", f"{total_quiebres:,}")
        else:
            st.info("No hay datos de costos cargados. Subí un archivo para comenzar.")
    except Exception as e:
        st.warning(f"No se pudo cargar el resumen: {str(e)}")
    
    st.divider()
    
    st.markdown("### 📋 Costos Cargados")
    
    try:
        costos_data = db.get_all_costos()
        
        if costos_data:
            df_costos_tabla = pd.DataFrame(costos_data)
            
            col_filter1, col_filter2 = st.columns(2)
            with col_filter1:
                filtro_codigo = st.text_input("🔍 Buscar por código", key="filtro_costo_cod")
            with col_filter2:
                filtro_desc = st.text_input("🔍 Buscar por descripción", key="filtro_costo_desc")
            
            if filtro_codigo:
                df_costos_tabla = df_costos_tabla[
                    df_costos_tabla['cod_articulo'].str.contains(filtro_codigo, case=False, na=False)
                ]
            if filtro_desc:
                df_costos_tabla = df_costos_tabla[
                    df_costos_tabla['descripcion'].astype(str).str.contains(filtro_desc, case=False, na=False)
                ]
            
            st.dataframe(
                df_costos_tabla,
                column_config={
                    'cod_articulo': st.column_config.TextColumn('Código', width='medium'),
                    'descripcion': st.column_config.TextColumn('Descripción', width='large'),
                    'costo_reposicion': st.column_config.NumberColumn('Costo', format='$%.2f'),
                    'moneda': st.column_config.TextColumn('Moneda', width='small'),
                    'fecha_actualizacion': st.column_config.DateColumn('Fecha Act.'),
                    'sync_timestamp': None
                },
                use_container_width=True,
                hide_index=True,
                height=400
            )
            
            st.caption(f"Mostrando {len(df_costos_tabla):,} de {len(costos_data):,} costos")
        else:
            st.info("No hay costos cargados todavía.")
    except Exception as e:
        st.warning(f"No se pudieron cargar los costos: {str(e)}")
