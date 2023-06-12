from resources.functions import *

def run():

	brawlers = pd.read_parquet('datasets/brawlers/brawlers.parquet')
	maps = pd.read_parquet('datasets/maps/maplist.parquet')
	metrics = pd.read_json('resources/bs_metrics.json', orient='index')
	means = pd.read_json('resources/means.json')

	means = round(means, 0)

	mean_brawler_trophies = int(means['mean_brawler_trophies'][0])
	mean_brawler_power = int(means['mean_brawler_power'][0])
	# mean_trophies = int(means['mean_trophies'][0])
	# mean_highestTrophies = int(means['mean_highestTrophies'][0])
	# mean_expPoints = int(means['mean_expPoints'][0])
	# mean_team_victories = int(means['mean_team_victories'][0])

	models_dict = load_models()

	st.sidebar.title('Data')
	st.sidebar.download_button('Download Brawlers Data', data=brawlers.to_csv(index=False), file_name='brawlers.csv')
	st.sidebar.subheader('Model Metrics')
	st.sidebar.dataframe(metrics, use_container_width=True)

	st.title('BrawlStars ML Model')
	st.write('This is a simple ML model that predicts the outcome of a BrawlStars match based on the brawlers and maps chosen.')
	st.write('The model is trained on data from the BrawlStars API, and the data is available in the sidebar.')

	ct = st.container()

	with ct:
		st.header('Game Settings')
		c1, c2 = st.columns(2)
		event_mode = c1.selectbox('Game Mode', maps['event_mode'].unique())
		event_map = c2.selectbox('Map', maps[maps['event_mode'] == event_mode]['event_map'])

		st.header('Team 1')
		c1, c2, c3 = st.columns(3)
		c1.subheader('Player 1')
		t1p1_name = c1.selectbox('Brawler', brawlers['name'], key='t1p1_name')
		t1p1_power = c1.slider('Power', 1, 11, mean_brawler_power, key='t1p1_power')
		t1p1_brawler_trophies = c1.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t1p1_brawler_trophies')
		# t1p1_expPoints = c1.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t1p1_expPoints')
		# t1p1_trophies = c1.slider('Trophies', 0, 100_000, mean_trophies, key='t1p1_trophies')
		# t1p1_highestTrophies = c1.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t1p1_highestTrophies')
		# t1p1_victories = c1.slider('Victories', 0, 50_000, mean_team_victories, key='t1p1_victories')
		c2.subheader('Player 2')
		t1p2_name = c2.selectbox('Brawler', brawlers['name'], key='t1p2_name')
		t1p2_power = c2.slider('Power', 1, 11, mean_brawler_power, key='t1p2_power')
		t1p2_brawler_trophies = c2.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t1p2_brawler_trophies')
		# t1p2_expPoints = c2.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t1p2_expPoints')
		# t1p2_trophies = c2.slider('Trophies', 0, 100_000, mean_trophies, key='t1p2_trophies')
		# t1p2_highestTrophies = c2.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t1p2_highestTrophies')
		# t1p2_victories = c2.slider('Victories', 0, 50_000, mean_team_victories, key='t1p2_victories')
		c3.subheader('Player 3')
		t1p3_name = c3.selectbox('Brawler', brawlers['name'], key='t1p3_name')
		t1p3_power = c3.slider('Power', 1, 11, mean_brawler_power, key='t1p3_power')
		t1p3_brawler_trophies = c3.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t1p3_brawler_trophies')
		# t1p3_expPoints = c3.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t1p3_expPoints')
		# t1p3_trophies = c3.slider('Trophies', 0, 100_000, mean_trophies, key='t1p3_trophies')
		# t1p3_highestTrophies = c3.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t1p3_highestTrophies')
		# t1p3_victories = c3.slider('Victories', 0, 50_000, mean_team_victories, key='t1p3_victories')

		st.header('Team 2')
		c1, c2, c3 = st.columns(3)
		c1.subheader('Player 1')
		t2p1_name = c1.selectbox('Brawler', brawlers['name'], key='t2p1_name')
		t2p1_power = c1.slider('Power', 1, 11, mean_brawler_power, key='t2p1_power')
		t2p1_brawler_trophies = c1.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p1_brawler_trophies')
		# t2p1_expPoints = c1.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t2p1_expPoints')
		# t2p1_trophies = c1.slider('Trophies', 0, 100_000, mean_trophies, key='t2p1_trophies')
		# t2p1_highestTrophies = c1.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t2p1_highestTrophies')
		# t2p1_victories = c1.slider('Victories', 0, 50_000, mean_team_victories, key='t2p1_victories')
		c2.subheader('Player 2')
		t2p2_name = c2.selectbox('Brawler', brawlers['name'], key='t2p2_name')
		t2p2_power = c2.slider('Power', 1, 11, mean_brawler_power, key='t2p2_power')
		t2p2_brawler_trophies = c2.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p2_brawler_trophies')
		# t2p2_expPoints = c2.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t2p2_expPoints')
		# t2p2_trophies = c2.slider('Trophies', 0, 100_000, mean_trophies, key='t2p2_trophies')
		# t2p2_highestTrophies = c2.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t2p2_highestTrophies')
		# t2p2_victories = c2.slider('Victories', 0, 50_000, mean_team_victories, key='t2p2_victories')
		c3.subheader('Player 3')
		t2p3_name = c3.selectbox('Brawler', brawlers['name'], key='t2p3_name')
		t2p3_power = c3.slider('Power', 1, 11, mean_brawler_power, key='t2p3_power')
		t2p3_brawler_trophies = c3.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p3_brawler_trophies')
		# t2p3_expPoints = c3.slider('Exp Points', 0, 1_000_000, mean_expPoints, key='t2p3_expPoints')
		# t2p3_trophies = c3.slider('Trophies', 0, 100_000, mean_trophies, key='t2p3_trophies')
		# t2p3_highestTrophies = c3.slider('Highest Trophies', 0, 100_000, mean_highestTrophies, key='t2p3_highestTrophies')
		# t2p3_victories = c3.slider('Victories', 0, 50_000, mean_team_victories, key='t2p3_victories')

	avg_brawler_trophies_diff = (t1p1_brawler_trophies + t1p2_brawler_trophies + t1p3_brawler_trophies) / 3 - (t2p1_brawler_trophies + t2p2_brawler_trophies + t2p3_brawler_trophies) / 3
	# avg_highestTrophies_diff = (t1p1_highestTrophies + t1p2_highestTrophies + t1p3_highestTrophies) / 3 - (t2p1_highestTrophies + t2p2_highestTrophies + t2p3_highestTrophies) / 3
	# avg_trophies_diff = (t1p1_trophies + t1p2_trophies + t1p3_trophies) / 3 - (t2p1_trophies + t2p2_trophies + t2p3_trophies) / 3
	# avg_team_victories_diff = (t1p1_victories + t1p2_victories + t1p3_victories) / 3 - (t2p1_victories + t2p2_victories + t2p3_victories) / 3
	# avg_expPoints_diff = (t1p1_expPoints + t1p2_expPoints + t1p3_expPoints) / 3 - (t2p1_expPoints + t2p2_expPoints + t2p3_expPoints) / 3
	max_brawler_trophies_diff = max(t1p1_brawler_trophies, t1p2_brawler_trophies, t1p3_brawler_trophies) - max(t2p1_brawler_trophies, t2p2_brawler_trophies, t2p3_brawler_trophies)
	# max_highestTrophies_diff = max(t1p1_highestTrophies, t1p2_highestTrophies, t1p3_highestTrophies) - max(t2p1_highestTrophies, t2p2_highestTrophies, t2p3_highestTrophies)
	# max_trophies_diff = max(t1p1_trophies, t1p2_trophies, t1p3_trophies) - max(t2p1_trophies, t2p2_trophies, t2p3_trophies)
	# max_team_victories_diff = max(t1p1_victories, t1p2_victories, t1p3_victories) - max(t2p1_victories, t2p2_victories, t2p3_victories)
	# max_expPoints_diff = max(t1p1_expPoints, t1p2_expPoints, t1p3_expPoints) - max(t2p1_expPoints, t2p2_expPoints, t2p3_expPoints)
	min_brawler_trophies_diff = min(t1p1_brawler_trophies, t1p2_brawler_trophies, t1p3_brawler_trophies) - min(t2p1_brawler_trophies, t2p2_brawler_trophies, t2p3_brawler_trophies)
	# min_highestTrophies_diff = min(t1p1_highestTrophies, t1p2_highestTrophies, t1p3_highestTrophies) - min(t2p1_highestTrophies, t2p2_highestTrophies, t2p3_highestTrophies)
	# min_trophies_diff = min(t1p1_trophies, t1p2_trophies, t1p3_trophies) - min(t2p1_trophies, t2p2_trophies, t2p3_trophies)
	# min_team_victories_diff = min(t1p1_victories, t1p2_victories, t1p3_victories) - min(t2p1_victories, t2p2_victories, t2p3_victories)
	# min_expPoints_diff = min(t1p1_expPoints, t1p2_expPoints, t1p3_expPoints) - min(t2p1_expPoints, t2p2_expPoints, t2p3_expPoints)
	battle_power_diff = t1p1_power + t1p2_power + t1p3_power - t2p1_power - t2p2_power - t2p3_power

	input_df = {
		'event_mode': event_mode,
		'event_map': event_map,
		'battle_team1_player1_brawler_name': t1p1_name,
		'battle_team1_player2_brawler_name': t1p2_name,
		'battle_team1_player3_brawler_name': t1p3_name,
		'battle_team2_player1_brawler_name': t2p1_name,
		'battle_team2_player2_brawler_name': t2p2_name,
		'battle_team2_player3_brawler_name': t2p3_name,
		'avg_brawler_trophies_diff': avg_brawler_trophies_diff,
		# 'avg_highestTrophies_diff': avg_highestTrophies_diff,
		# 'avg_trophies_diff': avg_trophies_diff,
		# 'avg_team_victories_diff': avg_team_victories_diff,
		# 'avg_expPoints_diff': avg_expPoints_diff,
		'max_brawler_trophies_diff': max_brawler_trophies_diff,
		# 'max_highestTrophies_diff': max_highestTrophies_diff,
		# 'max_trophies_diff': max_trophies_diff,
		# 'max_team_victories_diff': max_team_victories_diff,
		# 'max_expPoints_diff': max_expPoints_diff,
		'min_brawler_trophies_diff': min_brawler_trophies_diff,
		# 'min_highestTrophies_diff': min_highestTrophies_diff,
		# 'min_trophies_diff': min_trophies_diff,
		# 'min_team_victories_diff': min_team_victories_diff,
		# 'min_expPoints_diff': min_expPoints_diff,
		'battle_power_diff': battle_power_diff,
	}

	if st.button('Predict'):
		output = predict(pd.DataFrame(input_df, index=[0]), models_dict[event_mode])

		st.markdown(f"""
			# Prediction
			The winner is the team number **{output['prediction_label'].values}**
			The probability of winning is **{output['prediction_score'].values}**
		""")

if __name__ == '__main__':
	run()
