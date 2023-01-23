# %%
# importar paquetes a ocupar
import brawlstats
import pandas as pd
import datetime
# import ipywidgets as widgets
import zipfile
import os

# %%
# crear cliente
client = brawlstats.Client('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjdhMGNiNWIxLWFlMzctNDQzYi1iMTAzLWFmZjdmZDQwMDNhYyIsImlhdCI6MTY3NDMyMjUyNCwic3ViIjoiZGV2ZWxvcGVyL2Q0ZTc3OGNkLWJlYTAtZjlmNS04NDBhLTgzYTk1NTk3MWQ1MCIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiMjAxLjE4OC40LjM1Il0sInR5cGUiOiJjbGllbnQifV19.4nIoAAurRFujh0KHsgQ32E9xlycENhX4IqnS6btozJRkhZHnjgSH1YBhco9-CMq_Zk0OH10sMW9kjM_rZCkRIQ')
# Do not post your token on a public github

# %%
# importar brawlers
brawlers = pd.DataFrame()

try:
	json_brawlers = client.get_brawlers().raw_data
except:
	print("No se pudo recuperar listado de brawlers")

brawlers = pd.concat([brawlers, pd.json_normalize(json_brawlers)])

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
brawlerStats = pd.merge(brawlers['id'], brawlers_classification, on='id')
print(brawlerStats.info())

# %%
# export dataframe final brawlers
brawlerStats.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/brawlers/brawlers_stats.csv')

print('dimensiones brawlerStats: ' + str(brawlerStats.shape))

# %%
countryCode = ['US','MX','BR','GB','CA','DE','FR','ES','IT','RU','TR','AR','PL','CO','IN','ID','UA','AU','NL','JP','KR','CZ','CH','PH','MY']#,'VN','IE','TH','IL','NO','FI','PT','AT','GR','HU','SG','SA','AE','SE','DK','BZ','CR','GT','HN','NI','PA','SV','BO','CL','EC']
len(countryCode)

# %%
# sacar el player tag de los top 200 players
topplayer_tag=[]

# top global
leaderboard = client.get_rankings(ranking='players')
for i in leaderboard:
    topplayer_tag.append(i.tag)

# top por regiones en countryCode
for i, item in enumerate(countryCode):
   leaderboard = client.get_rankings(ranking='players',region=item)
   for i in leaderboard:
       topplayer_tag.append(i.tag)

topplayer_tag = list(set(topplayer_tag))

print('cantidad top player tag: ' + str(len(topplayer_tag)))

# %%
# importar battelog de api
# progress = widgets.IntProgress(
# 	value=0, 
# 	min=0, 
# 	max=100, 
# 	description='cargando:',
# 	style={'bar_color': 'maroon'}) 

# processed = widgets.BoundedFloatText(
#     value=0,
#     min=0,
#     max=len(topplayer_tag),
#     description='procesado:',
#     disabled=False
# )

# display(progress)
# display(processed)

data = {}

for i in range(len(topplayer_tag)):
    json_battlelog = {}
    playertag = topplayer_tag[i]
    try:
        json_battlelog = client.get_battle_logs(playertag).raw_data
    except:
        print("No se pudo recuperar battlelog de tag " + playertag)
    
    for k in range(len(json_battlelog)):
        loaded_json = json_battlelog[k]
        loaded_json['playertag'] = playertag
        data[str(i) + '-' + str(k)] = loaded_json

        # progress.value = ((i+1) / len(topplayer_tag)) * 100
        # processed.value = i+1

battlelog = pd.DataFrame.from_dict(data, orient='index').reset_index(drop=True)

# %%
# creación del dataframe
#battlelog = pd.DataFrame()

#def split_json(s):
#	# separa un string por el item "
#	s = str(s).split('"')
#
#	return s

#def clean_json(jsonitem):
#	# para una lista de artiuclos de json, separa en " y limpia los elementos impares de la lista resultante 
#	for i in range(len(jsonitem)):
#		splitted = split_json(jsonitem[i])
#		for j in range(len(splitted)):
#			splitted[j] = str(splitted[j]).replace('None','"None"').replace('False','"False"').replace('True','"True"')
#			if j % 2 == 1 and len(splitted[j]) < 20:
#				splitted[j] = '"' + str(splitted[j]).replace("'",'`') + '"'
#			else:
#				splitted[j] = str(splitted[j]).replace("'",'"')
#		jsonitem[i] = ''.join(splitted)
#	
#	return jsonitem

#def cleanjson(val):
#	result = str(val).replace('"',"`").replace(': `', ": '").replace('`,',"',").replace('`}',"'}").replace("e's",'e`s').replace('"ll','`ll').replace("I'm", 'I`m').replace("i'm", 'i`m').replace("' ","` ").replace("t's","t`s").replace('None',"'None'").replace("'",'"')
#	return result

# progress = widgets.IntProgress(
# 	value=0, 
# 	min=0, 
# 	max=100, 
# 	description='cargando:',
# 	style={'bar_color': 'maroon'}) 

# processed = widgets.BoundedFloatText(
#     value=0,
#     min=0,
#     max=len(topplayer_tag),
#     description='procesado:',
#     disabled=False
# )

# display(progress)
# display(processed)

# for i in range(len(topplayer_tag)):

# 	json_battlelog = {}
# 	playertag = topplayer_tag[i]
# 	try:
# 		json_battlelog = client.get_battle_logs(playertag).raw_data
# 	except:
# 		print("No se pudo recuperar battlelog de tag " + playertag)

# 	for k in range(len(json_battlelog)):
# 		loaded_json = json_battlelog[k]
# 		json_battlelog[k]['playertag'] = playertag
# 		try:
# 			battlelog = battlelog.append(pd.json_normalize(loaded_json), ignore_index=True)
# 		except:
# 			print("no se pudo importar " + playertag + " battlelog numero " + str(k))

# 	progress.value = ((i+1) / len(topplayer_tag)) * 100
# 	processed.value = i


# %%
# desglose de dataframe
battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['event']).add_prefix('event.'), left_index=True, right_index=True)
battlelog = pd.merge(left=battlelog, right=pd.json_normalize(battlelog['battle']).add_prefix('battle.'), left_index=True, right_index=True)

# %%
# reset battlelog index
battlelog.drop('event', inplace=True, axis=1)
battlelog.drop('battle', inplace=True, axis=1)
print('dimensiones battlelog: ' + str(battlelog.shape))

# %%
battlelog.info()

# %%
# Abrir el archivo comprimido
with zipfile.ZipFile('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.zip', 'r') as zip_ref:
    # Extraer el archivo CSV
    zip_ref.extractall('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/')

# Leer el archivo CSV
battlelog_complete = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv', index_col=0)

print('dimensiones battlelog complete: ' + str(battlelog_complete.shape))

battlelog_export = pd.concat([battlelog, battlelog_complete])

print('dimensiones battlelog concat: ' + str(battlelog_export.shape))

battlelog_export = battlelog_export.drop_duplicates(['playertag', 'battleTime', 'event.id', 'event.mode', 'event.map', 'battle.mode', 'battle.type', 'battle.result', 'battle.duration', 'battle.trophyChange'], ignore_index=True)

battlelog_export.reset_index(drop=True, inplace=True)

print('dimensiones battlelog export: ' + str(battlelog_export.shape))

battlelog_export.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv')

# Comprimir el archivo CSV
with zipfile.ZipFile('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.zip', 'w') as zip_file:
    zip_file.write('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv', arcname='battlelog_complete.csv',compress_type=zipfile.ZIP_DEFLATED)

os.remove('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/complete/battlelog_complete.csv')

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
# eliminar event vacio
battlelog['event.mode'] = battlelog['event.mode'].fillna('unknown')
battlelog = battlelog.loc[battlelog['event.mode'] != "unknown"]

# %%
# eliminar map vacio
battlelog['event.map'] = battlelog['event.map'].fillna('unknown')
battlelog = battlelog.loc[battlelog['event.map'] != "unknown"]

# %%
# reset battlelog index
battlelog.reset_index(drop=True, inplace=True)

print('dimensiones battlelog: ' + str(battlelog.shape))

# %%
# crear listado de players en battelog

#playerlist = pd.DataFrame(pd.concat([
#	battlelog['playertag'], 
#	battlelog['battle.starPlayer.tag'], 
#	battlelog['battle.team1.player1.tag'], 
#	battlelog['battle.team1.player2.tag'], 
#	battlelog['battle.team1.player3.tag'], 
#	battlelog['battle.team2.player1.tag'], 
#	battlelog['battle.team2.player2.tag'], 
#	battlelog['battle.team2.player3.tag']
#	], ignore_index=True).drop_duplicates().reset_index(drop=True))
#
#playerlist.shape

# %%
# crear listado nuevos playersplayers_hist
#playerlist_merge = pd.merge(playerlist, players_hist['tag'], left_on=0, right_on='tag', how='left').drop_duplicates().reset_index(drop=True)
#
#playerlist_final = playerlist_merge[0][playerlist_merge['tag'].isna()].drop_duplicates().reset_index(drop=True)
#
#playerlist_final.shape

# %%
# agregar datos de player
#players = pd.DataFrame()
#
#for i in range(len(playerlist_final)):
#
#	json_player = []
#	playertag = playerlist_final.loc[i]
#	try:
#		json_player = client.get_profile(playerlist_final.loc[i]).raw_data
#		del json_player['brawlers']
#	except:
#		print("No se pudo recuperar player de tag " + str(playertag))
#
#	try:
#		players = pd.concat([players, pd.json_normalize(json_player)])
#	except:
#		print("no se pudo importar player tag " + str(playertag))

# display(progress)
# display(processed)

timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

player = {}
json_player = []

for i, playertag in enumerate(topplayer_tag):
    try:
        json_player = client.get_profile(playertag).raw_data
        del json_player['brawlers']
        json_player['import_date'] = timestamp
        player[str(i)] = json_player
    except:
        print("No se pudo recuperar profile de tag " + str(playertag))

    # progress.value = ((i+1) / len(topplayer_tag)) * 100
    # processed.value = i+1

players = pd.DataFrame.from_dict(player, orient='index').reset_index(drop=True)

# %%
players_hist = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/players/players.csv', index_col=0)
players_hist.shape

# %%
# concatenar las bases
players = pd.concat([players, players_hist], ignore_index=True).drop_duplicates(['tag']).reset_index(drop=True)

print('dimensiones players: ' + str(players.shape))

# %%
players.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/players/players.csv')

# %%
# descomponer la columna teams

# def battle_delimiter(i, j):
# 	result = pd.json_normalize(pd.json_normalize(pd.json_normalize(pd.json_normalize(rawdata['battle'])['teams'])[i])[j])
# 	return result

def normalize_to_df(i, t, p):
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.tag'] = normalized[t - 1][p - 1]['tag']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.name'] = normalized[t - 1][p - 1]['name']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.id'] = normalized[t - 1][p - 1]['brawler.id']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.name'] = normalized[t - 1][p - 1]['brawler.name']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.power'] = normalized[t - 1][p - 1]['brawler.power']
	battlelog.loc[i,'battle.team' + str(t) + '.player' + str(p) + '.brawler.trophies'] = normalized[t - 1][p - 1]['brawler.trophies']

normalized = pd.DataFrame()

for i, team in enumerate(battlelog['battle.teams']):
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

battlelog.info()


# %%
battlelog.iloc[0].to_clipboard()

# %%
# eliminar battle teams
battlelog = battlelog.drop(columns=[
'battle.teams'
])

# %%
# traer archivo histórico battlelog
with zipfile.ZipFile('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.zip', 'r') as zip_ref:
    # Extraer el archivo CSV
    zip_ref.extractall('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/')
    
battlelog_hist = pd.read_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv', index_col=0)

print('dimensiones battlelog hist: ' + str(battlelog_hist.shape))

# %%
# agregar nuevos reg a histórico
battlelog = pd.concat([battlelog, battlelog_hist])
print('dimensiones battlelog concat: ' + str(battlelog.shape))

# %%
# eliminar battelogs duplicados
battlelog = battlelog.drop_duplicates(['battleTime', 'event.id', 'event.mode', 'event.map', 'battle.mode', 'battle.type', 'battle.duration', 'battle.team1.player1.tag'], ignore_index=True)

print('dimensiones battlelog final: ' + str(battlelog.shape))

# %%
# export dataset teams completo mas histórico
battlelog.to_csv('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv')

# Comprimir el archivo CSV
with zipfile.ZipFile('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.zip', 'w') as zip_file:
    zip_file.write('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv', arcname='battlelog_teams.csv',compress_type=zipfile.ZIP_DEFLATED)

os.remove('C:/Users/alniquia/OneDrive - Telefonica/Documents/Projects/BrawlStars_Model/datasets/teams/battlelog_teams.csv')


