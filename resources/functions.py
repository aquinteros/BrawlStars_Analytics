import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pycaret.classification import load_model, predict_model

st.set_page_config(layout="wide", page_title="BrawlStars ML Model")

@st.cache_resource()
def load_models():
    """Carga modelos de ML para predecir resultados"""
    modelos = {
        'brawlBall': load_model('models/bs_predictor_brawlBall'),
    }
    return modelos

def predict(input_df, model):
    return predict_model(estimator=model, data=input_df)