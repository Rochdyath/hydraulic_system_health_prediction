import os
import pandas as pd
import mlflow
import mlflow.sklearn
from airflow.decorators import dag, task
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Configuration
PROCESSED_DATA_PATH = "/data/processed/valve_condition_dataset.parquet"
MLFLOW_TRACKING_URI = "http://mlflow:5000"

default_args = {
    'owner': 'ml_engineer',
    'start_date': datetime(2025, 1, 1),
}

@task
def load_processed_data(file_path: str):
    """Tâche 1 : Charger le dataset préparé."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fichier manquant : {file_path}")
    return pd.read_parquet(file_path).to_dict(orient='list')

@task
def split_data(data_dict: dict):
    """Tâche 2 : Séparer les données (2000 premiers cycles pour train)."""
    df = pd.DataFrame(data_dict)
    
    # Split temporel strict selon l'énoncé
    train_df = df.iloc[:2000]
    test_df = df.iloc[2000:]
    
    return {
        "train": train_df.to_dict(orient='list'),
        "test": test_df.to_dict(orient='list')
    }

@task
def train_and_test_model(split_data_dict: dict):
    """Tâche 3 : Entraîner, Tester et Stocker dans MLflow."""
    train_df = pd.DataFrame(split_data_dict['train'])
    test_df = pd.DataFrame(split_data_dict['test'])
    
    X_train = train_df.drop(columns=['target'])
    y_train = train_df['target']
    X_test = test_df.drop(columns=['target'])
    y_test = test_df['target']
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("Maintenance_Valve_Project")
    
    with mlflow.start_run(run_name="RandomForest_test"):
        # Entraînement
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Test
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        # Log des métriques et paramètres
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("train_limit", 2000)
        mlflow.log_metric("accuracy_test_final", accuracy)
        
        # Stockage du modèle et versionnement
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="valve_model",
            registered_model_name="Valve_Condition_Classifier"
        )
        
        # Log du dataset comme artefact (pour le versionnement demandé)
        mlflow.log_artifact(PROCESSED_DATA_PATH, "versioned_dataset")
        
        print(f"Modèle prêt. Accuracy sur les cycles 2001-2205 : {accuracy:.4f}")
        return mlflow.get_artifact_uri("valve_model")

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['training', 'mlflow']
)
def valve_training_pipeline():

    # Dépendances
    raw_dict = load_processed_data(PROCESSED_DATA_PATH)
    splits = split_data(raw_dict)
    model_uri = train_and_test_model(splits)

# Instanciation
training_dag = valve_training_pipeline()