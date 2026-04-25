import pandas as pd

def extract_target(data):
    profile = pd.DataFrame(data)
    # Utilisation de "1" suite à la sérialisation JSON
    y = (profile["1"] == 100).astype(int)
    return y.tolist()


def extract_stats(df, name):
    stats = pd.DataFrame()
    stats[f'{name}_mean'] = df.mean(axis=1)
    stats[f'{name}_std'] = df.std(axis=1)
    stats[f'{name}_max'] = df.max(axis=1)
    stats[f'{name}_min'] = df.min(axis=1)
    return stats
