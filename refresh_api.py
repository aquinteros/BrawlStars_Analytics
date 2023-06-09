# ## Import packages
import brawlstats
import pandas as pd
import datetime
import api_key as key
import concurrent.futures as cf
from tqdm import tqdm
import json

# crear cliente
client = brawlstats.Client(key.api_key)

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

battlelog = pd.DataFrame.from_dict(data, orient='index').reset_index(drop=True)

battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['event'].tolist()).add_prefix('event.'), left_index=True, right_index=True)
battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['battle'].tolist()).add_prefix('battle.'), left_index=True, right_index=True)

battlelog = battlelog.drop('event', axis=1)
battlelog = battlelog.drop('battle', axis=1)
print('dimensiones battlelog: ' + str(battlelog.shape))

# cuenta tipos de juego
print(battlelog['battle.type'].value_counts())
# cuenta modos de juego
print(battlelog['battle.mode'].value_counts())

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

print('dimensiones battlelog: ' + str(battlelog.shape))

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

try:
	battlelog_hist = pd.read_parquet('datasets/teams/battlelog_teams.parquet')
except:
    battlelog_hist = pd.DataFrame()

print('dimensiones battlelog hist: ' + str(battlelog_hist.shape))

# agregar nuevos reg a histórico
battlelog = pd.concat([battlelog, battlelog_hist])
print('dimensiones battlelog concat: ' + str(battlelog.shape))

# eliminar battelogs duplicados
battlelog = battlelog.drop_duplicates(['battleTime', 'event_mode', 'event_map', 'battle_type', 'battle_duration', 'battle_team1_player1_tag'], ignore_index=True)

print('dimensiones battlelog final: ' + str(battlelog.shape))

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

# export dataset teams completo mas histórico
battlelog.to_parquet('datasets/teams/battlelog_teams.parquet', index=False, engine='fastparquet', compression='gzip')

maplist = battlelog[['event_mode','event_map']].drop_duplicates()

maplist.to_parquet('datasets/maps/maplist.parquet', index=False, engine='fastparquet', compression='gzip')


# define tags
# tags = pd.concat([
# 	battlelog['battle.team1.player1.tag']
# 	,battlelog['battle.team1.player2.tag']
# 	,battlelog['battle.team1.player3.tag']
# 	,battlelog['battle.team2.player1.tag']
# 	,battlelog['battle.team2.player2.tag']
# 	,battlelog['battle.team2.player3.tag']])
# tags = tags.drop_duplicates().reset_index(drop=True)
# tags.shape

# import players dataset
# player = {}

# top_player_list = tags.to_list()

# def get_profile(playertag):
# 	profile = client.get_profile(playertag)
# 	return {
# 		'tag': playertag, 
# 		'team_victories': profile.team_victories, 
# 		'highestTrophies': profile.highest_trophies, 
# 		'expPoints': profile.exp_points, 
# 		'trophies': profile.trophies,
# 		'datetime': datetime.datetime.now()
# 		}

# with cf.ThreadPoolExecutor(max_workers=40) as executor:
# 	future_to_player = {executor.submit(get_profile, playertag): playertag for playertag in top_player_list}
# 	for future in tqdm(cf.as_completed(future_to_player), total = len(top_player_list), colour='brown'):
# 		try:
# 			i = top_player_list.index(future_to_player[future])
# 			player[str(i)] = future.result()
# 		except:
# 			pass

# players = pd.DataFrame.from_dict(player, orient='index').reset_index(drop=True)

# importar historico de players
# players_hist = pd.read_parquet('datasets/players/players.parquet')

# print('dimensiones players hist: ' + str(players_hist.shape))


# concatenar las bases
# players = pd.concat([players_hist, players], ignore_index=True) \
# 	.drop_duplicates(subset='tag', keep='last') \
# 	.reset_index(drop=True)

# print('dimensiones players: ' + str(players.shape))


# export players
# players.to_parquet('datasets/players/players.parquet', index=False, engine='fastparquet', compression='gzip')
