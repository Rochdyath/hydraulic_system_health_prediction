import streamlit as st
import pandas as pd
import mlflow.sklearn
import os

# Configuration de la page
st.set_page_config(page_title="Valve Health Predictor", page_icon="🔧")

st.title("Prédiction de la Condition de la Valve")
st.markdown("""
Cette application prédit si la condition d'une valve hydraulique est **optimale (100%)** en se basant sur les données capteurs d'un cycle spécifique.
""")

# 1. Chargement des données et du modèle
@st.cache_resource
def load_resources():
    # Chargement du model via mlflow
    model_uri = "models:/Valve_Condition_Classifier/latest"
    model = mlflow.sklearn.load_model(model_uri)
    
    
    # Dataset pour récupérer les features par ID de cycle
    data_path = "/data/processed/valve_condition_dataset.parquet"
    df = pd.read_parquet(data_path)
    
    return df, model

df, model = load_resources()

# 2. Interface utilisateur
cycle_id = st.number_input("Entrez le numéro du cycle (0 - 2204) :", 
                           min_value=0, 
                           max_value=len(df)-1, 
                           value=2001)

if st.button("Prédire l'état"):
    # Extraction des features du cycle choisi
    features = df.iloc[[cycle_id]].drop(columns=['target'])
    actual_label = df.iloc[cycle_id]['target']
    
    # Prédiction
    prediction = model.predict(features)[0]
    
    st.subheader(f"Etat de la valve pour le cycle {cycle_id} :")
    
    if prediction == 1:
        st.success("OPTIMAL")
    else:
        st.error("NON-OPTIMAL")
        
    # Optionnel : Afficher les données du cycle
    with st.expander("Voir les caractéristiques du cycle"):
        st.write(features)
