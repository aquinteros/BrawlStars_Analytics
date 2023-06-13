from resources.functions import *

def run():

	events = ['brawlBall', 'heist', 'gemGrab', 'bounty', 'hotZone', 'knockout', 'volleyBrawl']

	metrics = pd.read_json('resources/bs_metrics.json', orient='index')

	st.title('Model Training')

	event_mode = st.selectbox('Game Mode', events)

	st.subheader('Train ML Model')

	if st.button('Train Model'):
		progress = st.progress(0)
		train_ml_model(event_mode, progress)
		st.success(f'Model trained for {event_mode}')
		
	st.subheader('Model Metrics')
	st.dataframe(metrics)
	

if __name__ == '__main__':
	run()