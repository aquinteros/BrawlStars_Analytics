from resources.functions import *
import datetime as dt

def run():

	data = pd.read_parquet('datasets/teams/battlelog_train.parquet')

	maxDate = data['battleTime'].dt.date.max()

	data['battleYearWeek'] = data['battleTime'].dt.strftime('%Y%U')

	st.title('API Import')
	st.subheader('Import Data from BrawlStars API')
	st.write('Data is imported from the BrawlStars API and preprocessed for ML model training.')
	st.write(f'The data available in the dataset is from games played unit {maxDate}.')

	if st.button('Import New Data'):

		progress = st.progress(0)
		api_import(progress)
		progress.progress(100)
		st.success('Data Imported')

		st.subheader('Preprocessing Data for ML Model')
		progress = st.progress(0)
		update_dataset(progress)
		progress.progress(100)
		st.success('Data Preprocessed')

	import plotly.express as px
	# import plotly.graph_objects as go
	
	px.defaults.template = "plotly_white"
	
	fig = px.histogram(data, x='event_mode', title='Games Played by Gamemode')

	st.plotly_chart(fig)

	fig = px.histogram(data, x='battleYearWeek', title='Games Played by Week')
	
	st.plotly_chart(fig)

	if st.button('Generate Download Link'):
		st.download_button('Download Data', convert_df(data), "predictions.csv", "text/csv", key='download-csv')

if __name__ == '__main__':
	run()