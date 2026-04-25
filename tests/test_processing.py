import pytest
import pandas as pd
import numpy as np
from airflow.plugins.utils import extract_target, extract_stats

def test_preprocess_target():
    # Simulation du dictionnaire sérialisé par Airflow
    fake_data = {"1": [100, 90, 80, 100]}
    expected = [1, 0, 0, 1]
    
    result = extract_target(fake_data)
    assert result == expected

def test_extract_stats_values():
    # Création d'un DataFrame de test : 2 cycles, 3 points par cycle
    # Cycle 0 : [10, 20, 30] -> Moyenne=20, Max=30, Min=10
    # Cycle 1 : [0, 0, 0]    -> Moyenne=0, Std=0
    data = {
        0: [10, 0],
        1: [20, 0],
        2: [30, 0]
    }
    df_test = pd.DataFrame(data)
    
    # Exécution
    name = "PS2"
    result = extract_stats(df_test, name)
    
    # Vérifications pour le Cycle 0
    assert result.loc[0, f'{name}_mean'] == 20.0
    assert result.loc[0, f'{name}_max'] == 30.0
    assert result.loc[0, f'{name}_min'] == 10.0
    
    # Vérification pour le Cycle 1 (Écart-type nul)
    assert result.loc[1, f'{name}_std'] == 0.0
    
    # Vérification de la structure
    expected_columns = [f'{name}_mean', f'{name}_std', f'{name}_max', f'{name}_min']
    assert list(result.columns) == expected_columns
    assert len(result) == 2

def test_extract_stats_empty():
    # Test avec un DataFrame vide pour vérifier la robustesse
    df_empty = pd.DataFrame()
    name = "FS1"
    result = extract_stats(df_empty, name)
    assert result.empty