import pandas as pd
from time import sleep
from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs

logs1 = TeamGameLogs(season_nullable='2015-16').get_data_frames()[0]
sleep(1)
logs2 = TeamGameLogs(season_nullable='2016-17').get_data_frames()[0]
sleep(1)
logs3 = TeamGameLogs(season_nullable='2017-18').get_data_frames()[0]
sleep(1)
logs4 = TeamGameLogs(season_nullable='2018-19').get_data_frames()[0]
sleep(1)
logs5 = TeamGameLogs(season_nullable='2019-20').get_data_frames()[0]
sleep(1)
logs6 = TeamGameLogs(season_nullable='2020-21').get_data_frames()[0]
sleep(1)
logs7 = TeamGameLogs(season_nullable='2021-22').get_data_frames()[0]

logs = pd.concat([logs1, logs2, logs3, logs4, logs5, logs6, logs7], axis=0)
logs.drop(logs.iloc[:, 30:], inplace=True, axis=1)

g_columns = ['SEASON_YEAR', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'MIN']
h_columns = ['TEAM_ID_(H)', 'TEAM_ABBREVIATION_(H)', 'TEAM_NAME_(H)', 'WL_(H)', 'FGM_(H)',
             'FGA_(H)', 'FG_PCT_(H)', 'FG3M_(H)', 'FG3A_(H)', 'FG3_PCT_(H)', 'FTM_(H)', 'FTA_(H)',
             'FT_PCT_(H)', 'OREB_(H)', 'DREB_(H)', 'REB_(H)', 'AST_(H)','TOV_(H)', 'STL_(H)', 'BLK_(H)',
             'BLKA_(H)', 'PF_(H)', 'PFD_(H)', 'PTS_(H)']
a_columns = ['TEAM_ID_(A)', 'TEAM_ABBREVIATION_(A)', 'TEAM_NAME_(A)', 'WL_(A)', 'FGM_(A)',
             'FGA_(A)', 'FG_PCT_(A)', 'FG3M_(A)', 'FG3A_(A)', 'FG3_PCT_(A)', 'FTM_(A)', 'FTA_(A)',
             'FT_PCT_(A)', 'OREB_(A)', 'DREB_(A)', 'REB_(A)', 'AST_(A)','TOV_(A)', 'STL_(A)', 'BLK_(A)',
             'BLKA_(A)', 'PF_(A)', 'PFD_(A)', 'PTS_(A)']
df = pd.DataFrame(columns=[g_columns+h_columns+a_columns+['DIFFERENCE']], index=[logs['GAME_ID'].unique()])

for i, row in logs.iterrows():
    if row['MATCHUP'].find('@') == -1:
        for col in row.index:
            if col in g_columns:
                df.at[row['GAME_ID'], col] = row[col]
            elif (col+'_(H)') in h_columns:
                df.at[row['GAME_ID'], (col+'_(H)')] = row[col]
    else:
        for col in row.index:
            if (col+'_(A)') in a_columns:
                df.at[row['GAME_ID'], (col+'_(A)')] = row[col]
for i, row in df.iterrows():
    df.at[i, 'DIFFERENCE'] = abs(row['PTS_(H)'] - row['PTS_(A)'])
    
df.to_csv('game_logs')