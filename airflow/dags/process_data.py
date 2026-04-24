from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import numpy as np
import os

# Configuration par défaut
default_args = {
    'owner': 'ml_engineer',
    'start_date': datetime(2025, 1, 1),
}

@task
def load_data():
    """
    Charge les fichiers bruts. 
    PS2: Pression (100 Hz) [cite: 28]
    FS1: Débit volumique (10 Hz) [cite: 29]
    """
    # Note: Dans un environnement réel, utilisez des chemins absolus ou S3/GCS
    ps2 = pd.read_csv('/data/raw/PS2.txt', sep='\t', header=None)
    fs1 = pd.read_csv('/data/raw/FS1.txt', sep='\t', header=None)
    profile = pd.read_csv('/data/raw/profile.txt', sep='\t', header=None)
    
    # On retourne un dictionnaire pour que TaskFlow puisse sérialiser
    return {
        "ps2": ps2.to_dict(orient='list'),
        "fs1": fs1.to_dict(orient='list'),
        "profile": profile.to_dict(orient='list')
    }

@task
def preprocess_target(data_dict: dict):
    """
    Prépare la cible binaire selon l'objectif de l'exercice.
    Optimal (100%) vs Non-optimal (90%, 80%, 73%).
    """
    profile = pd.DataFrame(data_dict['profile'])
    
    # La condition de la valve est dans la 2ème colonne du fichier profile [cite: 30, 12]
    # On crée un label binaire : 1 si optimal (100), 0 sinon 
    y = (profile["1"] == 100).astype(int)
    
    return y.tolist()

@task
def feature_engineering(data_dict: dict):
    """
    Agrège les données haute fréquence en statistiques par cycle.
    Chaque cycle dure 60 secondes[cite: 6].
    """
    ps2 = pd.DataFrame(data_dict['ps2'])
    fs1 = pd.DataFrame(data_dict['fs1'])
    
    def extract_stats(df, name):
        stats = pd.DataFrame()
        stats[f'{name}_mean'] = df.mean(axis=1)
        stats[f'{name}_std'] = df.std(axis=1)
        stats[f'{name}_max'] = df.max(axis=1)
        stats[f'{name}_min'] = df.min(axis=1)
        return stats

    # PS2 a 6000 attributs (100Hz) et FS1 en a 600 (10Hz) 
    features_ps2 = extract_stats(ps2, 'PS2')
    features_fs1 = extract_stats(fs1, 'FS1')
    
    X = pd.concat([features_ps2, features_fs1], axis=1)
    
    return X.to_dict(orient='list')

@task
def store_preprocessed_data(features_dict: dict, labels_list: list):
    """
    Fusionne features et labels, puis stocke le dataset final au format Parquet.
    """
    X = pd.DataFrame(features_dict)
    y = pd.Series(labels_list, name='target')
    
    # Fusion en un seul dataset pour la traçabilité
    dataset = pd.concat([X, y], axis=1)
    
    # Chemin de stockage persistant
    storage_path = "/data/processed/valve_condition_dataset.parquet"
    os.makedirs(os.path.dirname(storage_path), exist_ok=True)
    dataset.to_parquet(storage_path, index=False)

    return storage_path

@dag(
    default_args=default_args,
    schedule_interval=None,
    # catch_year=False,
    description="Pipeline de maintenance prédictive pour l'état de la valve"
)
def data_pipeline():
    # Définition du flux de données
    raw_data = load_data()
    labels = preprocess_target(raw_data)
    features = feature_engineering(raw_data)
    store_data = store_preprocessed_data(features, labels)


# Instanciation du DAG
projet_dag = data_pipeline()