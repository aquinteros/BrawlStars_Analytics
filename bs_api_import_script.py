# %%
#%pip install brawlstats

# %%
# importar paquetes a ocupar
import brawlstats
import pandas as pd
import numpy as np
import json

# %%
# crear cliente
client = brawlstats.Client('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImQ5MmQ4NDdhLWI0ZjAtNDVjNC04MzAwLWMxYzg1YmYxNGRjYSIsImlhdCI6MTY3MjkzMTY0MCwic3ViIjoiZGV2ZWxvcGVyL2Q0ZTc3OGNkLWJlYTAtZjlmNS04NDBhLTgzYTk1NTk3MWQ1MCIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiMjAxLjE4OC4yNS4xODYiXSwidHlwZSI6ImNsaWVudCJ9XX0.FmhW24C4spfT1KZ2XP88I3GcQZ7mxwSBdWz77NBwS-f928QAom4e1VKs_lYKa_0WEz5BVQFXrbYJB3ghk4NRww')
# Do not post your token on a public github

# %%
# sacar el player tag de los top 200 players
topplayer_tag=[]
leaderboard = client.get_rankings(ranking='players')
for i in leaderboard:
    topplayer_tag.append(i.tag)

print(topplayer_tag)

# %%
# creación del dataframe
battlelog = pd.DataFrame()

def split_json(s):
	# separa un string por el item "
	s = str(s).split('"')

	return s

def clean_json(jsonitem):
	# para una lista de artiuclos de json, separa en " y limpia los elementos impares de la lista resultante 
	for i in range(len(jsonitem)):
		splitted = split_json(jsonitem[i])
		for j in range(len(splitted)):
			splitted[j] = str(splitted[j]).replace('None','"None"')
			if j % 2 == 1 and len(splitted[j]) < 20:
				splitted[j] = '"' + str(splitted[j]).replace("'",'`') + '"'
			else:
				splitted[j] = str(splitted[j]).replace("'",'"')
		jsonitem[i] = ''.join(splitted)
	
	return jsonitem

#def cleanjson(val):
#	result = str(val).replace('"',"`").replace(': `', ": '").replace('`,',"',").replace('`}',"'}").replace("e's",'e`s').replace('"ll','`ll').replace("I'm", 'I`m').replace("i'm", 'i`m').replace("' ","` ").replace("t's","t`s").replace('None',"'None'").replace("'",'"')
#	return result

for i in range(len(topplayer_tag)):

	playertag = topplayer_tag[i]
	try:
		json_battlelog = client.get_battle_logs(playertag).to_list()
	except:
		print("No se pudo recuperar battlelog de tag " + playertag)

	json_battlelog = clean_json(json_battlelog)

	for k in range(len(json_battlelog)):
		try:
			loaded_json = json.loads('{"playertag": "' + playertag + '", ' + json_battlelog[k][1:])
			battlelog = pd.concat([battlelog, pd.json_normalize(loaded_json)])
		except:
			print("no se pudo importar " + playertag + " battlelog numero " + str(k))
	

# %%
# ver info del dataset
print(battlelog.info())

# %%
# reset battlelog index
battlelog.reset_index(drop=True, inplace=True)

print(battlelog.shape)

# %%
# export dataset completo
battlelog_complete = pd.read_csv('datasets/complete/battlelog_complete.csv', index_col=0)

battlelog_export = pd.concat([battlelog, battlelog_complete])

battlelog_export = battlelog_export.drop_duplicates(['playertag', 'battle_time', 'event.id', 'event.mode', 'event.map', 'battle.mode', 'battle.type', 'battle.result', 'battle.duration', 'battle.trophy_change'], ignore_index=True)

battlelog_export.reset_index(drop=True, inplace=True)

battlelog_export.to_csv('datasets/complete/battlelog_complete.csv')

# %%
# cuenta tipos de juego
battlelog['battle.type'].value_counts()

# %%
# eliminar friendly
battlelog = battlelog.loc[battlelog['battle.type'] != "friendly"]

# %%
# cuenta modos de juego
battlelog['battle.mode'].value_counts() 

# %%
# eliminar modos alt y modos showdown

modos_alt = ['bossFight','roboRumble','bigGame','soloShowdown','duoShowdown']

battlelog = battlelog.loc[~battlelog['battle.mode'].isin(modos_alt)]

# %%
# eliminar columnas nulas
battlelog = battlelog.drop(columns=[
'battle.rank'
,'battle.players'
,'battle.big_brawler.tag'
,'battle.big_brawler.name'
,'battle.big_brawler.brawler.id'
,'battle.big_brawler.brawler.name'
,'battle.big_brawler.brawler.power'
,'battle.big_brawler.brawler.trophies'
,'battle.star_player'
])

battlelog.info()

# %%
# reset battlelog index
battlelog.reset_index(drop=True, inplace=True)

print(battlelog.shape)

# %%
# descomponer la columna teams

def normalize_to_df(i, t, p):
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.tag'] = normalized[t - 1][p - 1]['tag']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.name'] = normalized[t - 1][p - 1]['name']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.id'] = normalized[t - 1][p - 1]['brawler.id']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.name'] = normalized[t - 1][p - 1]['brawler.name']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.power'] = normalized[t - 1][p - 1]['brawler.power']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.trophies'] = normalized[t - 1][p - 1]['brawler.trophies']

normalized = pd.DataFrame()

for i in range(len(battlelog)):
	team = battlelog['battle.teams'].iloc[i]
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


# %%
# eliminar battle teams
battlelog = battlelog.drop(columns=[
'battle.teams'
])

print(battlelog.info())

# %%
# traer archivo histórico battlelog
battlelog_hist = pd.read_csv('datasets/teams/battlelog_teams.csv', index_col=0)
print(battlelog_hist.shape)

# %%
# agregar nuevos reg a histórico
battlelog = pd.concat([battlelog, battlelog_hist])
print(battlelog.shape)

# %%
# eliminar battelogs duplicados
battlelog = battlelog.drop_duplicates(ignore_index=True)
print(battlelog.shape)

# %%
# export dataset teams completo mas histórico
battlelog.to_csv('datasets/teams/battlelog_teams.csv')

# %%
# importar brawlers
brawlers = pd.DataFrame()

try:
	json_brawlers = client.get_brawlers().to_list()
except:
	print("No se pudo recuperar listado de brawlers")

json_brawlers = clean_json(json_brawlers)

for k in range(len(json_brawlers)):
	try:
		loaded_json = json.loads(json_brawlers[k])
		brawlers = pd.concat([brawlers, pd.json_normalize(loaded_json)])
	except:
		print("no se pudo importar brawler numero " + str(k))
		print(json_brawlers[k])

# %%
# reset brawler index y export de dataset
brawlers.reset_index(drop=True, inplace=True)

brawlers.to_csv('datasets/brawlers/brawlers.csv')

print(brawlers)

# %%
# import información adicional de brawlers
brawlers_classification = pd.read_csv('datasets/brawlers/brawlers_classification.csv', index_col=0)

print(brawlers_classification.head())

# %%
# merge de ambos dataframes
brawlerStats = pd.merge(brawlers, brawlers_classification)
print(brawlerStats.info())

# %%
# export dataframe final brawlers
brawlerStats.to_csv('datasets/brawlers/brawlers_stats.csv')


