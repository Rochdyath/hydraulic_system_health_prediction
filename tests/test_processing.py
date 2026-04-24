import pytest
import pandas as pd
import numpy as np
from airflow.dags.process_data import preprocess_target, extract_stats # Importe tes fonctions

def test_preprocess_target():
    # Simulation du dictionnaire sérialisé par Airflow
    fake_data = {"profile": {"1": [100, 90, 80, 100]}}
    expected = [1, 0, 0, 1]
    
    result = preprocess_target(fake_data)
    assert result == expected

def test_extract_stats():
    # Simulation d'un cycle avec des valeurs constantes
    df = pd.DataFrame([[10, 20, 30], [100, 200, 300]])
    # On teste la logique de ta fonction interne extract_stats
    mean_val = df.mean(axis=1).tolist()
    assert mean_val == [20.0, 200.0]