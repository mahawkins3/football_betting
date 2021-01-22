import requests
import pandas as pd
import io
import datetime as dt
import numpy as np

#Source URLs for data
url_18 = "https://raw.githubusercontent.com/mahawkins3/football_betting/main/epl/data/EPL_Set.csv"
url_19 = "https://www.football-data.co.uk/mmz4281/1819/E0.csv"
url_20 = "https://www.football-data.co.uk/mmz4281/1920/E0.csv"
url_21 = "https://www.football-data.co.uk/mmz4281/2021/E0.csv"

#Ingest data
res = requests.get(url_18, allow_redirects=True)
with open('EPL_Set.csv', 'wb') as file:
    file.write(res.content)

res_18 = pd.read_csv('EPL_Set.csv')
s_19 = requests.get(url_19).content
res_19 = pd.read_csv(io.StringIO(s_19.decode('utf-8')))
s_20 = requests.get(url_20).content
res_20 = pd.read_csv(io.StringIO(s_20.decode('utf-8')))
s_21 = requests.get(url_21).content
res_21 = pd.read_csv(io.StringIO(s_21.decode('utf-8')))

#Make season column consistent
res_18['Season'] = res_18['Season'].apply(lambda x: x[:4])
res_19['Season'] = '2018'
res_20['Season'] = '2019'
res_21['Season'] = '2020'

#Need to make res_18 date format consistent as source data is a mess
res_18_days = res_18['Date'].apply(lambda x: x.split('/')[0].zfill(2))
res_18_months = res_18['Date'].apply(lambda x: x.split('/')[1].zfill(2))
years = res_18['Date'].apply(lambda x: x.split('/', 2)[2]).tolist()
res_18_years = []
for y in years:
    if len(y) == 4:
        res_18_years.append(y)
    elif int(y) >= 93:
        res_18_years.append('19' + y)
    else:
        res_18_years.append('20' + y)

res_18['Date'] = res_18_days + '/' + res_18_months + '/' + res_18_years

#Discard columns that we don't need
keep = ["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "Season"]
res_18 = res_18[keep]
res_19 = res_19[keep]
res_20 = res_20[keep]
res_21 = res_21[keep]

#Concatenate tables
res_all = pd.concat([res_18, res_19, res_20, res_21])

#Convert dates to datetime format
res_all['Date'] = pd.to_datetime(res_all['Date'], infer_datetime_format=True)

#Overall result column
conditions = [
(res_all['FTHG'] > res_all['FTAG']),
(res_all['FTAG'] > res_all['FTHG']),
(res_all['FTHG'] == res_all['FTAG'])
]
values = ['H', 'A', 'D']
res_all['Result'] = np.select(conditions, values)
res_all['MatchId'] = np.arange(res_all.shape[0])

#Split data into home teams and away teams
home_teams = res_all[['MatchId', 'Date', 'HomeTeam', 'Season', 'Result']].copy()
home_teams.columns = ['MatchId', 'Date', 'Team', 'Season', 'Result']
home_teams['Type'] = 'Home'
away_teams = res_all[['MatchId', 'Date', 'AwayTeam', 'Season', 'Result']].copy()
away_teams.columns = ['MatchId', 'Date', 'Team', 'Season', 'Result']
away_teams['Type'] = 'Away'

match_dates = pd.concat([home_teams,away_teams])
match_dates['MatchRank'] = (match_dates.groupby(['Team'])['MatchId']
    .rank(method='first').astype(int))
match_dates['JoinKey'] = match_dates['Team'] + match_dates['MatchRank'].astype(str)

#Match to each team's previous match to calculate date difference
match_dates_right = match_dates.copy()
match_dates_right['MatchRank'] += 1
match_dates_right["JoinKey"] = (match_dates_right['Team']
    + match_dates_right['MatchRank'].astype(str))
match_dates_right = match_dates_right[['JoinKey','Date']]

match_dates = match_dates.merge(match_dates_right, on = 'JoinKey', how = 'inner',
    suffixes = ('', '_prev'))

#Calculate days since team's last match

match_dates['DaysSinceLast'] = (match_dates['Date'] - match_dates['Date_prev']).dt.days

##NEED TO ADD THING TO CALCULATE POINTS IN EACH TEAM'S LAST FEW GAMES

match_dates = match_dates[['MatchId', 'Type', 'DaysSinceLast']]

match_dates_home = match_dates[match_dates['Type'] == 'Home']
match_dates_away = match_dates[match_dates['Type'] == 'Away']

res_all = (res_all.merge(match_dates_home[['MatchId', 'DaysSinceLast']], on = 'MatchId', how = 'left')
    .merge(match_dates_away[['MatchId', 'DaysSinceLast']], on = 'MatchId', suffixes = ('_home', '_away'), how = 'left'))



print(res_all)
