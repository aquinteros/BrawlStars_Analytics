from resources.functions import *

def run():

	brawlers = pd.read_parquet('datasets/brawlers/brawlers.parquet')
	maps = pd.read_parquet('datasets/maps/maplist.parquet')
	metrics = pd.read_json('resources/bs_metrics.json', orient='index')
	means = pd.read_json('resources/means.json')

	means = round(means, 0)

	mean_brawler_trophies = int(means['mean_brawler_trophies'][0])
	mean_brawler_power = int(means['mean_brawler_power'][0])

	models_dict = load_models()

	st.sidebar.title('Data')
	st.sidebar.download_button('Download Brawlers Data', data=brawlers.to_csv(index=False), file_name='brawlers.csv')
	st.sidebar.subheader('Model Metrics')
	st.sidebar.dataframe(metrics, use_container_width=True)

	st.sidebar.title('API Import')
	st.sidebar.subheader('Import Data from BrawlStars API')
	if st.sidebar.button('Import Data'):

		progress = st.sidebar.progress(0)
		api_import(progress)
		progress.progress(100)
		st.sidebar.success('Data Imported')

	st.sidebar.title('Data Preprocessing')
	st.sidebar.subheader('Preprocess Data for ML Model')
	if st.sidebar.button('Preprocess Data'):
		progress = st.sidebar.progress(0)
		update_dataset(progress)
		progress.progress(100)
		st.sidebar.success('Data Preprocessed')

	st.sidebar.title('Model Training')
	st.sidebar.subheader('Train ML Model')
	if st.sidebar.button('Train Model'):
		progress = st.sidebar.progress(0)
		train_ml_model(progress)
		st.sidebar.success('Model Trained')


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
		c2.subheader('Player 2')
		t1p2_name = c2.selectbox('Brawler', brawlers['name'], key='t1p2_name')
		t1p2_power = c2.slider('Power', 1, 11, mean_brawler_power, key='t1p2_power')
		t1p2_brawler_trophies = c2.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t1p2_brawler_trophies')
		c3.subheader('Player 3')
		t1p3_name = c3.selectbox('Brawler', brawlers['name'], key='t1p3_name')
		t1p3_power = c3.slider('Power', 1, 11, mean_brawler_power, key='t1p3_power')
		t1p3_brawler_trophies = c3.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t1p3_brawler_trophies')

		st.header('Team 2')
		c1, c2, c3 = st.columns(3)
		c1.subheader('Player 1')
		t2p1_name = c1.selectbox('Brawler', brawlers['name'], key='t2p1_name')
		t2p1_power = c1.slider('Power', 1, 11, mean_brawler_power, key='t2p1_power')
		t2p1_brawler_trophies = c1.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p1_brawler_trophies')
		c2.subheader('Player 2')
		t2p2_name = c2.selectbox('Brawler', brawlers['name'], key='t2p2_name')
		t2p2_power = c2.slider('Power', 1, 11, mean_brawler_power, key='t2p2_power')
		t2p2_brawler_trophies = c2.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p2_brawler_trophies')
		c3.subheader('Player 3')
		t2p3_name = c3.selectbox('Brawler', brawlers['name'], key='t2p3_name')
		t2p3_power = c3.slider('Power', 1, 11, mean_brawler_power, key='t2p3_power')
		t2p3_brawler_trophies = c3.slider('Brawler Trophies', 0, 1_500, mean_brawler_trophies, key='t2p3_brawler_trophies')

	avg_brawler_trophies_diff = (t1p1_brawler_trophies + t1p2_brawler_trophies + t1p3_brawler_trophies) / 3 - (t2p1_brawler_trophies + t2p2_brawler_trophies + t2p3_brawler_trophies) / 3
	max_brawler_trophies_diff = max(t1p1_brawler_trophies, t1p2_brawler_trophies, t1p3_brawler_trophies) - max(t2p1_brawler_trophies, t2p2_brawler_trophies, t2p3_brawler_trophies)
	min_brawler_trophies_diff = min(t1p1_brawler_trophies, t1p2_brawler_trophies, t1p3_brawler_trophies) - min(t2p1_brawler_trophies, t2p2_brawler_trophies, t2p3_brawler_trophies)
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
		'max_brawler_trophies_diff': max_brawler_trophies_diff,
		'min_brawler_trophies_diff': min_brawler_trophies_diff,
		'battle_power_diff': battle_power_diff,
	}

	if st.button('Predict'):

		for brawler in brawlers['name']:
			input_df['T1_' + brawler] = 0
			input_df['T2_' + brawler] = 0
		
		input_df['T1_' + t1p1_name] = input_df['T1_' + t1p1_name] + 1
		input_df['T1_' + t1p2_name] = input_df['T1_' + t1p2_name] + 1
		input_df['T1_' + t1p3_name] = input_df['T1_' + t1p3_name] + 1
		input_df['T2_' + t2p1_name] = input_df['T2_' + t2p1_name] + 1
		input_df['T2_' + t2p2_name] = input_df['T2_' + t2p2_name] + 1
		input_df['T2_' + t2p3_name] = input_df['T2_' + t2p3_name] + 1

		input_df = pd.DataFrame(input_df, index=[0])

		input_df = input_df.drop(columns=[
			'battle_team1_player1_brawler_name', 
			'battle_team1_player2_brawler_name', 
			'battle_team1_player3_brawler_name', 
			'battle_team2_player1_brawler_name', 
			'battle_team2_player2_brawler_name', 
			'battle_team2_player3_brawler_name', 
		])

		output = predict(input_df, models_dict[event_mode])

		st.markdown(f"""
			# Prediction
			The winner is the team number **{output['prediction_label'].values}**
			The probability of winning is **{output['prediction_score'].values}**
		""")

if __name__ == '__main__':
	run()
