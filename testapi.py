import requests

url = 'https://bsproxy.royaleapi.dev/v1/'
# url = 'https://api.brawlstars.com/v1/'
api_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImQ0YWFmZTc1LTFiYTEtNGQyNi04ZGY4LTE3N2UyMjQxYjQ0OSIsImlhdCI6MTY4NjY4MTM0OSwic3ViIjoiZGV2ZWxvcGVyL2Q0ZTc3OGNkLWJlYTAtZjlmNS04NDBhLTgzYTk1NTk3MWQ1MCIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNDUuNzkuMjE4Ljc5Il0sInR5cGUiOiJjbGllbnQifV19.9eP3UQFYUQLFOcLBE1l82fFpHJ-0VbbLY6TKZJ9e1qIBbfy35lBuVLId_mJNU_GW8IwMyjEnZJ8-SbU-56Dnlg'
# api_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjQ0NjJmMDFkLWZmZDMtNDc0MC1iNDZhLTUyZTE0ZjNjOWZmMyIsImlhdCI6MTY4NjY4OTE0MSwic3ViIjoiZGV2ZWxvcGVyL2Q0ZTc3OGNkLWJlYTAtZjlmNS04NDBhLTgzYTk1NTk3MWQ1MCIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiMjAwLjI4LjI1NS4yNyJdLCJ0eXBlIjoiY2xpZW50In1dfQ.mvthHuAZSU_tgU5Nhaa7qT-5Y4qRhZNbWW8uRSyu9t-qUXGxLLH1A7YYsXxDD7kxcUlJDVqYyFqf-Qgp2mvWBg'

def get_battlelog(tag):
	r = requests.get(url + 'players/%23' + tag + '/battlelog', headers={'Authorization': 'Bearer ' + api_key})
	return r.json()

print(get_battlelog('802QPYCQ8'))

# player = '#802QPYCQ8'