import pandas as pd
import numpy as np
import streamlit as st
# import plotly.express as px
# import plotly.graph_objects as go
import brawlstats
import concurrent.futures as cf
import json
from tqdm import tqdm
from pycaret.classification import load_model, predict_model

st.set_page_config(page_title="BrawlStars ML Model")

@st.cache_resource()
def load_models():
    """Carga modelos de ML para predecir resultados"""
    modelos = {
        'brawlBall': load_model('models/bs_predictor_brawlBall'),
        'gemGrab': load_model('models/bs_predictor_gemGrab'),
    }
    return modelos

def predict(input_df, model):
    return predict_model(estimator=model, data=input_df)

def api_import(progress):
    """Importa datos de la API de BrawlStars"""
    client = brawlstats.Client(st.secrets['api_key'])

    # importar brawlers
    json_brawlers = client.get_brawlers().raw_data
    brawlers = pd.json_normalize(json_brawlers)[['id', 'name']]

    # reset brawler index y export de dataset
    brawlers.to_parquet('datasets/brawlers/brawlers.parquet', index=False, engine='fastparquet', compression='gzip')

    # import información adicional de brawlers
    brawlers_classification = pd.read_csv('datasets/brawlers/brawlers_classification.csv', index_col=0)

    # merge de ambos dataframes
    brawlerStats = pd.merge(brawlers, brawlers_classification, on='id')

    # export dataframe final brawlers
    brawlerStats.to_parquet('datasets/brawlers/brawlers_stats.parquet', index=False, engine='fastparquet', compression='gzip')

    print('dimensiones brawlerStats: ' + str(brawlerStats.shape))

    countryCode = ['US','MX','BR','GB','CA','DE','FR','ES','IT','RU','TR','AR','PL','CO','IN','ID','UA','AU','NL','JP','KR','CZ','CH','PH','MY','VN','IE','TH','IL','NO','FI','PT','AT','GR','HU','SG','SA','AE','SE','DK','BZ','CR','GT','HN','NI','PA','SV','BO','CL','EC']
    
    # sacar el player tag de los top players
    top_player = []

    # top global
    leaderboard = client.get_rankings(ranking='players')
    for i in leaderboard:
        top_player.append({'tag': i.tag, 'trophies': i.trophies, 'rank_type': 'global'})

    # top por regiones en countryCode
    for i, item in enumerate(countryCode):
        leaderboard = client.get_rankings(ranking='players',region=item)
        for k in leaderboard:
            top_player.append({'tag': k.tag, 'trophies': k.trophies, 'rank_type': item})

    top_player = pd.DataFrame(top_player).drop_duplicates(subset='tag', keep='first').reset_index(drop=True)

    print('cantidad top player tag: ' + str(len(top_player)))

    # exportar dataset en parquet
    top_player.to_parquet('datasets/players/top_player.parquet', index=False, engine='fastparquet', compression='gzip')

    # sacar el club de los top clubs
    top_club = []

    # top global
    leaderboard = client.get_rankings(ranking='clubs')
    for i in leaderboard:
        top_club.append({'tag': i.tag, 'trophies': i.trophies, 'rank_type': 'global'})

    # top por regiones en countryCode
    for i, item in enumerate(countryCode):
        leaderboard = client.get_rankings(ranking='players',region=item)
        for k in leaderboard:
            top_club.append({'tag': k.tag, 'trophies': k.trophies, 'rank_type': 'region'})

    top_club = pd.DataFrame(top_club).drop_duplicates(subset='tag', keep='first').reset_index(drop=True)

    print('cantidad top club tag: ' + str(len(top_club)))

    # exportar dataset en parquet
    top_club.to_parquet('datasets/clubs/top_club.parquet', index=False, engine='fastparquet', compression='gzip')

    # importar battlelog usando concurrent.futures
    def process_player(i, playertag):
        json_battlelog = {}
        try:
            json_battlelog = client.get_battle_logs(playertag).raw_data
        except:
            print("No se pudo recuperar battlelog de tag " + playertag)

        player_data = {}

        for k in range(len(json_battlelog)):
            loaded_json = json_battlelog[k]
            loaded_json['playertag'] = playertag
            player_data[str(i) + '-' + str(k)] = loaded_json
        return player_data

    data = {}

    top_player_list = top_player['tag'].to_list()

    print(f'Inicio de proceso de recuperación de battlelog de {len(top_player_list)} jugadores')

    with cf.ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(process_player, i, playertag) for i, playertag in enumerate(top_player_list)]
        for future in tqdm(cf.as_completed(futures), total=len(top_player_list)):
            player_data = future.result()
            data.update(player_data)
        progress.progress(0.5)

    battlelog = pd.DataFrame.from_dict(data, orient='index').reset_index(drop=True)

    battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['event'].tolist()).add_prefix('event.'), left_index=True, right_index=True)
    battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['battle'].tolist()).add_prefix('battle.'), left_index=True, right_index=True)

    battlelog = battlelog.drop('event', axis=1)
    battlelog = battlelog.drop('battle', axis=1)
    print('dimensiones battlelog: ' + str(battlelog.shape))

    print(f'Se eliminan {len(battlelog.loc[battlelog["battle.type"] == "friendly"])} eventos de tipo friendly')
    battlelog = battlelog.loc[battlelog['battle.type'] != "friendly"]

    modos_alt = ['bossFight','roboRumble','bigGame','soloShowdown','duoShowdown', 'duels']

    print(f'Se eliminan {len(battlelog.loc[battlelog["battle.mode"].isin(modos_alt)])} eventos de modos alternativos')
    battlelog = battlelog.loc[~battlelog['battle.mode'].isin(modos_alt)]

    battlelog['event.mode'] = battlelog['event.mode'].fillna('unknown')
    print(f'Se eliminan {len(battlelog.loc[battlelog["event.mode"] == "unknown"])} eventos sin modo de juego')
    battlelog = battlelog.loc[battlelog['event.mode'] != "unknown"]

    battlelog['event.map'] = battlelog['event.map'].fillna('unknown')
    print(f'Se eliminan {len(battlelog.loc[battlelog["event.map"] == "unknown"])} eventos sin mapa')
    battlelog = battlelog.loc[battlelog['event.map'] != "unknown"]

    # reset battlelog index
    battlelog = battlelog.reset_index(drop=True)

    def normalize_to_df(i, t, p):
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.tag'] = normalized[t - 1][p - 1]['tag']
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.name'] = normalized[t - 1][p - 1]['name']
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.id'] = normalized[t - 1][p - 1]['brawler.id']
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.name'] = normalized[t - 1][p - 1]['brawler.name']
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.power'] = normalized[t - 1][p - 1]['brawler.power']
        battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.trophies'] = normalized[t - 1][p - 1]['brawler.trophies']

    normalized = pd.DataFrame()

    print(f'Inicio de proceso de normalización de battlelog')

    for i, team in tqdm(enumerate(battlelog['battle.teams']), total=len(battlelog['battle.teams'])):
        progress.progress((i / len(battlelog['battle.teams'])) / 2 + 0.5)
        if team != None:
            try:
                normalized = pd.json_normalize(team, errors='ignore').transpose()
                normalize_to_df(i, 1, 1)
                normalize_to_df(i, 1, 2)
                normalize_to_df(i, 1, 3)
                normalize_to_df(i, 2, 1)
                normalize_to_df(i, 2, 2)
                normalize_to_df(i, 2, 3)
            except:
                print("no se pudo transponer")

    # fix column names
    battlelog.columns = battlelog.columns.str.replace('.', '_', regex=True)

    # select columns
    battlelog = battlelog[[
    'battleTime'
    ,'playertag'
    ,'event_mode'
    ,'event_map'
    ,'battle_type'
    ,'battle_result'
    ,'battle_duration'
    ,'battle_trophyChange'
    ,'battle_team1_player1_tag'
    ,'battle_team1_player1_name'
    ,'battle_team1_player1_brawler_id'
    ,'battle_team1_player1_brawler_name'
    ,'battle_team1_player1_brawler_power'
    ,'battle_team1_player1_brawler_trophies'
    ,'battle_team1_player2_tag'
    ,'battle_team1_player2_name'
    ,'battle_team1_player2_brawler_id'
    ,'battle_team1_player2_brawler_name'
    ,'battle_team1_player2_brawler_power'
    ,'battle_team1_player2_brawler_trophies'
    ,'battle_team1_player3_tag'
    ,'battle_team1_player3_name'
    ,'battle_team1_player3_brawler_id'
    ,'battle_team1_player3_brawler_name'
    ,'battle_team1_player3_brawler_power'
    ,'battle_team1_player3_brawler_trophies'
    ,'battle_team2_player1_tag'
    ,'battle_team2_player1_name'
    ,'battle_team2_player1_brawler_id'
    ,'battle_team2_player1_brawler_name'
    ,'battle_team2_player1_brawler_power'
    ,'battle_team2_player1_brawler_trophies'
    ,'battle_team2_player2_tag'
    ,'battle_team2_player2_name'
    ,'battle_team2_player2_brawler_id'
    ,'battle_team2_player2_brawler_name'
    ,'battle_team2_player2_brawler_power'
    ,'battle_team2_player2_brawler_trophies'
    ,'battle_team2_player3_tag'
    ,'battle_team2_player3_name'
    ,'battle_team2_player3_brawler_id'
    ,'battle_team2_player3_brawler_name'
    ,'battle_team2_player3_brawler_power'
    ,'battle_team2_player3_brawler_trophies'
    ]]


    dtypes = {
        'battleTime': 'datetime64[ns]',
        'battle_duration': 'Int16',
        'battle_trophyChange': 'Int8',
        'battle_team1_player1_brawler_id': 'Int32',
        'battle_team1_player1_brawler_power': 'Int8',
        'battle_team1_player1_brawler_trophies': 'Int16',
        'battle_team1_player2_brawler_id': 'Int32',
        'battle_team1_player2_brawler_power': 'Int8',
        'battle_team1_player2_brawler_trophies': 'Int16',
        'battle_team1_player3_brawler_id': 'Int32',
        'battle_team1_player3_brawler_power': 'Int8',
        'battle_team1_player3_brawler_trophies': 'Int16',
        'battle_team2_player1_brawler_id': 'Int32',
        'battle_team2_player1_brawler_power': 'Int8',
        'battle_team2_player1_brawler_trophies': 'Int16',
        'battle_team2_player2_brawler_id': 'Int32',
        'battle_team2_player2_brawler_power': 'Int8',
        'battle_team2_player2_brawler_trophies': 'Int16',
        'battle_team2_player3_brawler_id': 'Int32',
        'battle_team2_player3_brawler_power': 'Int8',
        'battle_team2_player3_brawler_trophies': 'Int16',
        'battle_team1_player1_brawler_name': 'category',
        'battle_team1_player2_brawler_name': 'category',
        'battle_team1_player3_brawler_name': 'category',
        'battle_team2_player1_brawler_name': 'category',
        'battle_team2_player2_brawler_name': 'category',
        'battle_team2_player3_brawler_name': 'category',
        'battle_result': 'category',
        'event_mode': 'category',
        'event_map': 'category',
        'battle_type': 'category',
        'battle_result': 'category',
    }

    battlelog = battlelog.astype(dtypes)

    print(f'dimensiones battlelog: {battlelog.shape}')

    try:
        battlelog_hist = pd.read_parquet('datasets/teams/battlelog_teams.parquet')
    except:
        battlelog_hist = pd.DataFrame()

    print('dimensiones battlelog hist: ' + str(battlelog_hist.shape))

    # agregar nuevos reg a histórico
    battlelog_final = pd.concat([battlelog, battlelog_hist])
    print('dimensiones battlelog concat: ' + str(battlelog_final.shape))

    # eliminar battelogs duplicados
    battlelog_final = battlelog_final.drop_duplicates(['battleTime', 'event_mode', 'event_map', 'battle_type', 'battle_duration', 'battle_team1_player1_tag'], ignore_index=True)

    print('dimensiones battlelog final sin duplicados: ' + str(battlelog_final.shape))

    # export dataset teams completo mas histórico
    battlelog_final.to_parquet('datasets/teams/battlelog_teams.parquet', index=False, engine='fastparquet', compression='gzip')

    maplist = battlelog_final[['event_mode','event_map']].drop_duplicates()

    maplist.to_parquet('datasets/maps/maplist.parquet', index=False, engine='fastparquet', compression='gzip')

def team_assignment(row):
    """Función para identificar a que team pertenece el player"""
    if any(row['playertag'] == tag for tag in [row.battle_team1_player1_tag, row.battle_team1_player2_tag, row.battle_team1_player3_tag]):
        return 1
    elif any(row['playertag'] == tag for tag in [row.battle_team2_player1_tag, row.battle_team2_player2_tag, row.battle_team2_player3_tag]):
        return 2
    else:
        print("error in team assignation for id " + str(row.name))

def winner_assignment(row):
    """Función para identificar si el team del player es el ganador"""
    if row.battle_result == 'draw':
        return 'draw'
    elif (row.player_team == 1 and row.battle_result == 'victory') or (row.player_team == 2 and row.battle_result == 'defeat'):
        return 1
    elif (row.player_team == 2 and row.battle_result == 'victory') or (row.player_team == 1 and row.battle_result == 'defeat'):
        return 2
    else:
        return "error in winner assignation for id " + str(row.name)

def create_avg(row, column, team):
    """Función para calcular el promedio de un atributo de los 3 players del team"""
    return (row['battle_' + team + '_player1_'+ column] + row['battle_' + team + '_player2_'+ column] + row['battle_' + team + '_player3_'+ column]) / 3

def get_min_max_rest(row, column_names):
    """Función para obtener el min, max y rest de un atributo de los 3 players del team"""
    a, b, c = row[column_names[0]], row[column_names[1]], row[column_names[2]]
    min_val = min(a, b, c)
    max_val = max(a, b, c)
    # rest_val = (a + b + c) - min_val - max_val
    # return min_val, max_val, rest_val
    return min_val, max_val

def order_rows(df, column, team):
    """Función para ordenar las filas de un dataframe según el valor de una columna"""
    column_names = ['battle_' + team + '_player1_' + column, 'battle_' + team + '_player2_' + column, 'battle_' + team + '_player3_' + column]
    # min_vals, max_vals, rest_vals = zip(*df.apply(lambda x: get_min_max_rest(x, column_names), axis=1))
    min_vals, max_vals = zip(*df.apply(lambda x: get_min_max_rest(x, column_names), axis=1))
    return min_vals, max_vals

def to_log(df, column):
    """Función para aplicar logaritmo a una columna de un dataframe"""
    df[column] = np.log(df[column] + 1)
    return df[column]

def min_max_values(series, min_value, max_value):
    """Función para reemplazar los valores de una serie que estén fuera de un rango por el valor máximo o mínimo"""
    series = series.apply(lambda x: max_value if x > max_value else (min_value if x < min_value else x))
    
    return series

def eliminar_outliers(df, columna):
    """Función para eliminar outliers de una columna de un dataframe"""
    q1 = df[columna].quantile(0.25)
    q3 = df[columna].quantile(0.75)
    iqr = q3 - q1
    umbral_inferior  = q1 - 1.5 * iqr
    umbral_superior = q3 + 1.5 * iqr
    df[columna] = min_max_values(df[columna], umbral_inferior, umbral_superior)
    return df

def update_dataset(progress):
    """Función para actualizar el dataset de battlelog"""

    df = pd.read_parquet('datasets/teams/battlelog_teams.parquet')

    df['player_team'] = df.apply(team_assignment, axis=1)
    df = df[df['battle_result'] != 'draw'].reset_index(drop=True)
    df['winner_team'] = df.apply(winner_assignment, axis=1)

    progress.progress(20)

    for team in ['team1', 'team2']:
        df['avg_brawler_trophies_' + team] = df.apply(lambda row: create_avg(row, 'brawler_trophies', team), axis=1)

    df = df[(df['avg_brawler_trophies_team1'] > 50) & (df['avg_brawler_trophies_team2'] > 50)].reset_index(drop = True)

    progress.progress(40)

    for team in ['team1', 'team2']:
        df['min_brawler_trophies_' + team], df['max_brawler_trophies_' + team] = order_rows(df, 'brawler_trophies', team)

    for team in ['team1', 'team2']:
        for prefix in ['avg', 'min', 'max']:
            df[prefix + '_brawler_trophies_' + team] = to_log(df, prefix + '_brawler_trophies_' + team)
            df = eliminar_outliers(df, prefix + '_brawler_trophies_' + team)

    progress.progress(60)

    for team in ['team1', 'team2']:
        df['battle_power_' + team] = df['battle_' + team + '_player1_brawler_power'] + df['battle_' + team + '_player2_brawler_power'] + df['battle_' + team + '_player3_brawler_power']

    cols = ['battle_power', 'avg_brawler_trophies', 'min_brawler_trophies', 'max_brawler_trophies']

    progress.progress(80)

    for col in cols:
        df[col + '_diff'] = df[col + '_team1'] - df[col + '_team2']

    df.to_parquet('datasets/teams/battlelog_train.parquet', index=False, compression='gzip')

def split_data(data, test_size, random_state):
    """Funcion para dividir el dataset en train y test"""
    from sklearn.model_selection import train_test_split

    train, test = train_test_split(
            data,
            test_size=test_size,
            random_state=random_state
            )
        
    print('train: ', train.shape)
    print('test: ', test.shape)

    return train, test

def metrics_capturing(df):
	"""Captures the metrics of a classification model"""
	from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

	accuracy = accuracy_score(df['winner_team'], df['prediction_label'])
	f1 = f1_score(df['winner_team'], df['prediction_label'])
	precision = precision_score(df['winner_team'], df['prediction_label'])
	recall = recall_score(df['winner_team'], df['prediction_label'])

	return accuracy, f1, precision, recall

def train_ml_model(event, progress):
    """"Función para entrenar el modelo de machine learning"""
    from pycaret.classification import create_model, setup, finalize_model, predict_model, save_model

    df = pd.read_parquet('datasets/teams/battlelog_train.parquet')

    df = df[[
        'event_mode',
        'event_map',
        'battle_team1_player1_brawler_name',
        'battle_team1_player2_brawler_name',
        'battle_team1_player3_brawler_name',
        'battle_team2_player1_brawler_name',
        'battle_team2_player2_brawler_name',
        'battle_team2_player3_brawler_name',
        'avg_brawler_trophies_diff',
        'max_brawler_trophies_diff',
        'min_brawler_trophies_diff',
        'battle_power_diff',
        'winner_team',
    ]]

    # dummy brawler name
    dft1 = pd.get_dummies(data=df['battle_team1_player1_brawler_name'], prefix='T1') + \
        pd.get_dummies(data=df['battle_team1_player2_brawler_name'], prefix='T1') + \
        pd.get_dummies(data=df['battle_team1_player3_brawler_name'], prefix='T1')

    dft2 = pd.get_dummies(data=df['battle_team2_player1_brawler_name'], prefix='T2') + \
        pd.get_dummies(data=df['battle_team2_player2_brawler_name'], prefix='T2') + \
        pd.get_dummies(data=df['battle_team2_player3_brawler_name'], prefix='T2')

    df = pd.concat([df, dft1, dft2], axis=1)
    df = df.drop(columns=[
        'battle_team1_player1_brawler_name',
        'battle_team1_player2_brawler_name',
        'battle_team1_player3_brawler_name',
        'battle_team2_player1_brawler_name',
        'battle_team2_player2_brawler_name',
        'battle_team2_player3_brawler_name',
    ])

    events = ['brawlBall', 'heist', 'gemGrab', 'bounty', 'hotZone', 'knockout', 'volleyBrawl']

    print(f'Entrenando modelo para {event}...')
    dataset = df[df['event_mode'] == event]
    dataset = dataset.drop(columns=['event_mode'])
    
    seed=14697

    train, test = split_data(dataset, test_size = 0.25, random_state=seed)

    session_1 = setup(
        data = train,
        target = 'winner_team',
        # fix_imbalance = True,
        # feature_selection= True,
        # remove_outliers=True,
        log_experiment = True,
        use_gpu=False,
        max_encoding_ohe=500,
    )

    progress.progress(0.2)

    model = create_model('lightgbm')

    progress.progress(0.5)

    model_finalized = finalize_model(model)

    save_model(model_finalized, 'models/bs_predictor_' + event)

    predictions = predict_model(model_finalized, data = test)
    predictions['winner_team'] = predictions['winner_team'] + 1

    accuracy, f1, precision, recall = metrics_capturing(predictions)

    metrics = {
            'accuracy': accuracy,
            'f1': f1,
            'precision': precision,
            'recall': recall,
    }

    print(metrics)

    with open('resources/bs_metrics.json', 'r') as f:
        data = json.load(f)
        data[event] = metrics

    with open('resources/bs_metrics.json', 'w') as f:
        json.dump(data, f, indent=4)

    progress.progress(100)