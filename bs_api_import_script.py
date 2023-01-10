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
client = brawlstats.Client('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjNiNThjYWY4LWRmMDUtNDFiYy05NDVhLWNjYjJhNjE3YTViMiIsImlhdCI6MTY3MzM2MDM0Niwic3ViIjoiZGV2ZWxvcGVyL2Q0ZTc3OGNkLWJlYTAtZjlmNS04NDBhLTgzYTk1NTk3MWQ1MCIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiMTg2LjE0OC4zLjIwMCJdLCJ0eXBlIjoiY2xpZW50In1dfQ.LM3TNmmmYq-8A3GAgpCz8AKC4Q918oFGra4x7ZiOoOVR16wACy467AaAILPihjFCiWO0o3mILM5HRqxjmSuOog')
# Do not post your token on a public github

# %%
# sacar el player tag de los top 200 players
topplayer_tag=[]
leaderboard = client.get_rankings(ranking='players')
for i in leaderboard:
    topplayer_tag.append(i.tag)

print('cantidad top player tag: ' + str(len(topplayer_tag)))

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

print('dimensiones battlelog: ' + str(battlelog.shape))

# %%
# export dataset completo
battlelog_complete = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv', index_col=0)

print('dimensiones battlelog complete: ' + str(battlelog_complete.shape))

battlelog_export = pd.concat([battlelog, battlelog_complete])

battlelog_export = battlelog_export.drop_duplicates(['playertag', 'battle_time', 'event.id', 'event.mode', 'event.map', 'battle.mode', 'battle.type', 'battle.result', 'battle.duration', 'battle.trophy_change'], ignore_index=True)

battlelog_export.reset_index(drop=True, inplace=True)

print('dimensiones battlelog export: ' + str(battlelog_export.shape))

battlelog_export.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv')

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
# reset battlelog index
battlelog.reset_index(drop=True, inplace=True)

print('dimensiones battlelog: ' + str(battlelog.shape))

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

# %%
# traer archivo histórico battlelog
battlelog_hist = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv', index_col=0)

print('dimensiones battlelog hist: ' + str(battlelog_hist.shape))

# %%
# agregar nuevos reg a histórico
battlelog = pd.concat([battlelog, battlelog_hist])
print('dimensiones battlelog concat: ' + str(battlelog.shape))

# %%
# eliminar battelogs duplicados
battlelog = battlelog.drop_duplicates(['battle_time', 'event.id', 'event.mode', 'event.map', 'battle.mode', 'battle.type', 'battle.duration', 'battle.team1.player1.tag'], ignore_index=True)

print('dimensiones battlelog final: ' + str(battlelog.shape))

# %%
# export dataset teams completo mas histórico
battlelog.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv')

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

brawlers.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/brawlers/brawlers.csv')

print(brawlers.info())

# %%
# import información adicional de brawlers
brawlers_classification = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/brawlers/brawlers_classification.csv', index_col=0)

# %%
# merge de ambos dataframes
brawlerStats = pd.merge(brawlers, brawlers_classification)
print(brawlerStats.info())

# %%
# export dataframe final brawlers
brawlerStats.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/brawlers/brawlers_stats.csv')

print('dimensiones brawlerStats: ' + str(brawlerStats.shape))


