from datetime import datetime, timedelta
from textwrap import dedent
import time
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing
import requests
import lxml

from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


####################################################
# DEFINE PYTHON FUNCTIONS
####################################################

def fetch_all_new_data(**kwargs):
    df = pd.read_csv('~/airflow/dags/Player Totals.csv')
    df = df[df['season'] == 2023]
    player_names = df['player'].values.reshape(1,-1)[0]
    player_ids = []
    player_arr = []
    player_info = players.get_active_players()
    for i in range(len(player_info)):
        if player_info[i]['full_name'] in player_names:
            player_ids.append(player_info[i]['id'])
            player_arr.append(player_info[i]['full_name'])
            
    df_players = None
    for i in range(len(player_ids)):
        p_id = player_ids[i]
        player_stats = playercareerstats.PlayerCareerStats(player_id=p_id)
        df = player_stats.get_data_frames()[0]
        df = df[df['SEASON_ID'] == '2022-23']
        df_players = pd.concat([df_players, df])
        
    # preprocess new data
    df_players['player'] = player_arr
    df_players['orb_pg'] = df_players['OREB']/df_players['GP']
    df_players['drb_pg'] = df_players['DREB']/df_players['GP']
    df_players['ast_pg'] = df_players['AST']/df_players['GP']
    df_players['stl_pg'] = df_players['STL']/df_players['GP']
    df_players['blk_pg'] = df_players['BLK']/df_players['GP']
    df_players['tov_pg'] = df_players['TOV']/df_players['GP']
    df_players['pts_pg'] = df_players['PTS']/df_players['GP']
    
    # save player stats to a csv file
    df_stats = df_players[['player', 'GP', 'GS', 'FG_PCT', 'FG3_PCT', 'FT_PCT', 'pts_pg']]
    df_stats.to_csv('~/airflow/dags/2023 Players.csv', index=None)
    return df_players

def fetch_rookie_new_data(**kwargs):
    df = pd.read_csv('~/airflow/dags/Player Totals.csv')
    df = df[df['season'] == 2023]
    df = df[df['experience'] <= 1]
    player_names = df['player'].values.reshape(1,-1)[0]
    player_ids = []
    player_arr = []
    player_info = players.get_active_players()
    for i in range(len(player_info)):
        if player_info[i]['full_name'] in player_names:
            player_ids.append(player_info[i]['id'])
            player_arr.append(player_info[i]['full_name'])
            
    df_players = None
    for i in range(len(player_ids)):
        p_id = player_ids[i]
        player_stats = playercareerstats.PlayerCareerStats(player_id=p_id)
        df = player_stats.get_data_frames()[0]
        df = df[df['SEASON_ID'] == '2022-23']
        df_players = pd.concat([df_players, df])
        
    # preprocess new data
    df_players['player'] = player_arr
    df_players['orb_pg'] = df_players['OREB']/df_players['GP']
    df_players['drb_pg'] = df_players['DREB']/df_players['GP']
    df_players['ast_pg'] = df_players['AST']/df_players['GP']
    df_players['stl_pg'] = df_players['STL']/df_players['GP']
    df_players['blk_pg'] = df_players['BLK']/df_players['GP']
    df_players['tov_pg'] = df_players['TOV']/df_players['GP']
    df_players['pts_pg'] = df_players['PTS']/df_players['GP']
    return df_players

def fetch_sm_new_data(**kwargs):
    df = pd.read_csv('~/airflow/dags/Player Totals.csv')
    df = df[df['season'] == 2023]
    df = df[df['gs'] < 0.5 * df['g']]
    player_names = df['player'].values.reshape(1,-1)[0]
    player_ids = []
    player_arr = []
    player_info = players.get_active_players()
    for i in range(len(player_info)):
        if player_info[i]['full_name'] in player_names:
            player_ids.append(player_info[i]['id'])
            player_arr.append(player_info[i]['full_name'])
            
    df_players = None
    for i in range(len(player_ids)):
        p_id = player_ids[i]
        player_stats = playercareerstats.PlayerCareerStats(player_id=p_id)
        df = player_stats.get_data_frames()[0]
        df = df[df['SEASON_ID'] == '2022-23']
        df_players = pd.concat([df_players, df])
        
    # preprocess new data
    df_players['player'] = player_arr
    df_players['orb_pg'] = df_players['OREB']/df_players['GP']
    df_players['drb_pg'] = df_players['DREB']/df_players['GP']
    df_players['ast_pg'] = df_players['AST']/df_players['GP']
    df_players['stl_pg'] = df_players['STL']/df_players['GP']
    df_players['blk_pg'] = df_players['BLK']/df_players['GP']
    df_players['tov_pg'] = df_players['TOV']/df_players['GP']
    df_players['pts_pg'] = df_players['PTS']/df_players['GP']
    return df_players

def fetch_mvp_training(**kwargs):
    player_data = pd.read_csv("~/airflow/dags/Player Totals.csv")
    award_data = pd.read_csv("~/airflow/dags/Player Award Shares.csv")
    award_data = award_data[award_data["award"] == 'nba mvp']
    player_data.insert(0, 'MVP', False)
    for index, row in award_data.iterrows():
        player_data['MVP'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , True, player_data['MVP'])
    player_data = player_data.fillna(0)
    player_data = player_data[player_data['MVP'] == True]
    award_data = award_data[award_data["winner"] == False].reset_index(drop=True)
    for index, row in award_data.iterrows():
        player_data['MVP'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , False, player_data['MVP'])
    training_mvp = player_data[player_data['season'] >= 1980]
    training_mvp = player_data[player_data['season'] != 2023]
    return training_mvp

def fetch_dpoy_training(**kwargs):
    player_data = pd.read_csv("~/airflow/dags/Player Totals.csv")
    award_data = pd.read_csv("~/airflow/dags/Player Award Shares.csv")
    award_data = award_data[award_data["award"] == 'dpoy']
    player_data.insert(0, 'dpoy', False)
    for index, row in award_data.iterrows():
        player_data['dpoy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , True, player_data['dpoy'])
    player_data = player_data.fillna(0)
    player_data = player_data[player_data['dpoy'] == True]
    award_data = award_data[award_data["winner"] == False].reset_index(drop=True)
    for index, row in award_data.iterrows():
        player_data['dpoy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , False, player_data['dpoy'])
    training_dpoy = player_data[player_data['season'] >= 1980]
    training_dpoy = player_data[player_data['season'] != 2023]
    return training_dpoy

def fetch_roy_training(**kwargs):
    player_data = pd.read_csv("~/airflow/dags/Player Totals.csv")
    award_data = pd.read_csv("~/airflow/dags/Player Award Shares.csv")
    award_data = award_data[award_data["award"] == 'nba roy']
    player_data.insert(0, 'roy', False)
    for index, row in award_data.iterrows():
        player_data['roy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , True, player_data['roy'])
    player_data = player_data.fillna(0)
    player_data = player_data[player_data['roy'] == True]
    award_data = award_data[award_data["winner"] == False].reset_index(drop=True)
    for index, row in award_data.iterrows():
        player_data['roy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , False, player_data['roy'])
    training_roy = player_data[player_data['season'] >= 1980]
    training_roy = player_data[player_data['season'] != 2023]
    return training_roy

def fetch_smoy_training(**kwargs):
    player_data = pd.read_csv("~/airflow/dags/Player Totals.csv")
    award_data = pd.read_csv("~/airflow/dags/Player Award Shares.csv")
    award_data = award_data[award_data["award"] == 'smoy']
    player_data.insert(0, 'smoy', False)
    for index, row in award_data.iterrows():
        player_data['smoy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , True, player_data['smoy'])
    player_data = player_data.fillna(0)
    player_data = player_data[player_data['MVP'] == True]
    award_data = award_data[award_data["winner"] == False].reset_index(drop=True)
    for index, row in award_data.iterrows():
        player_data['smoy'] = np.where((player_data['season'] == row['season']) & (player_data['player'] == row['player']) , False, player_data['smoy'])
    training_smoy = player_data[player_data['season'] >= 1980]
    training_smoy = player_data[player_data['season'] != 2023]
    return training_smoy

def train_mvp(**kwargs):
    ti = kwargs['ti']
    training_mvp = ti.xcom_pull(task_ids='fetch_MVP_training_data')
    X_train = training_mvp[["fg", "fg_percent",	"x3p",	"x3p_percent",	"orb",	"drb", "ast",	"stl",	"blk", "pts", "orb_per_game", "drb_per_game", "ast_per_game", "stl_per_game", "blk_per_game", "pts_per_game"]].values
    y_train = training_mvp['MVP'].values
    X_train = preprocessing.StandardScaler().fit_transform(X_train)
    mvp_model = LogisticRegression(random_state=0).fit(X_train, y_train)
    return mvp_model

def train_dpoy(**kwargs):
    ti = kwargs['ti']
    training_dpoy = ti.xcom_pull(task_ids='fetch_DPOY_training_data')
    X_train = training_dpoy[["drb",	"stl",	"blk", "drb_per_game", "stl_per_game", "blk_per_game"]].values
    y_train = training_dpoy['dpoy'].values
    X_train = preprocessing.StandardScaler().fit_transform(X_train)
    dpoy_model = LogisticRegression(random_state=0).fit(X_train, y_train)
    return dpoy_model

def train_roy(**kwargs):
    ti = kwargs['ti']
    training_roy = ti.xcom_pull(task_ids='fetch_ROY_training_data')
    X_train = training_roy[["fg", "fg_percent",	"x3p",	"x3p_percent",	"orb",	"drb", "ast",	"stl",	"blk", "pts", "orb_per_game", "drb_per_game", "ast_per_game", "stl_per_game", "blk_per_game", "pts_per_game"]].values
    y_train = training_roy['roy'].values
    X_train = preprocessing.StandardScaler().fit_transform(X_train)
    roy_model = LogisticRegression(random_state=0).fit(X_train, y_train)
    return roy_model

def train_smoy(**kwargs):
    ti = kwargs['ti']
    training_smoy = ti.xcom_pull(task_ids='fetch_SMOY_training_data')
    X_train = training_smoy[["fg", "fg_percent",	"x3p",	"x3p_percent",	"orb",	"drb", "ast",	"stl",	"blk", "pts", "orb_per_game", "drb_per_game", "ast_per_game", "stl_per_game", "blk_per_game", "pts_per_game"]].values
    y_train = training_smoy['smoy'].values
    X_train = preprocessing.StandardScaler().fit_transform(X_train)
    smoy_model = RandomForestClassifier(max_depth=10, random_state=0).fit(X_train, y_train)
    return smoy_model

def predict_dpoy(**kwargs):
    ti = kwargs['ti']
    df_players = ti.xcom_pull(task_ids='fetch_new_player_data')
    mvp_model = ti.xcom_pull(task_ids='train_DPOY_model')
    test_data = df_players[["DREB", "STL", "BLK", "drb_pg", "stl_pg", "blk_pg"]].values
    test_data = preprocessing.StandardScaler().fit_transform(test_data)
    predictions = mvp_model.predict_proba(test_data)
    p_dpoy = predictions[:,1].reshape(-1,1)
    return p_dpoy
    
def predict_mvp(**kwargs):
    ti = kwargs['ti']
    df_players = ti.xcom_pull(task_ids='fetch_new_player_data')
    mvp_model = ti.xcom_pull(task_ids='train_MVP_model')
    test_data = df_players[["FGM", "FG_PCT", "FG3M", "FG3_PCT", "OREB", "DREB", "AST", "STL", "BLK", "PTS", "orb_pg", "drb_pg", "ast_pg", "stl_pg", "blk_pg", "pts_pg"]].values
    test_data = preprocessing.StandardScaler().fit_transform(test_data)
    predictions = mvp_model.predict_proba(test_data)
    p_mvp = predictions[:,1].reshape(-1,1)
    return df_players, p_mvp

def predict_roy(**kwargs):
    ti = kwargs['ti']
    df_players = ti.xcom_pull(task_ids='fetch_new_rookie_data')
    mvp_model = ti.xcom_pull(task_ids='train_ROY_model')
    test_data = df_players[["FGM", "FG_PCT", "FG3M", "FG3_PCT", "OREB", "DREB", "AST", "STL", "BLK", "PTS", "orb_pg", "drb_pg", "ast_pg", "stl_pg", "blk_pg", "pts_pg"]].values
    test_data = preprocessing.StandardScaler().fit_transform(test_data)
    predictions = mvp_model.predict_proba(test_data)
    p_roy = predictions[:,1].reshape(-1,1)
    return df_players, p_roy

def predict_smoy(**kwargs):
    ti = kwargs['ti']
    df_players = ti.xcom_pull(task_ids='fetch_new_substitute_data')
    mvp_model = ti.xcom_pull(task_ids='train_SMOY_model')
    test_data = df_players[["FGM", "FG_PCT", "FG3M", "FG3_PCT", "OREB", "DREB", "AST", "STL", "BLK", "PTS", "orb_pg", "drb_pg", "ast_pg", "stl_pg", "blk_pg", "pts_pg"]].values
    test_data = preprocessing.StandardScaler().fit_transform(test_data)
    predictions = mvp_model.predict_proba(test_data)
    p_smoy = predictions[:,1].reshape(-1,1)
    return df_players, p_smoy

def save_results(**kwargs):
    ti = kwargs['ti']
    df_players, p_mvp = ti.xcom_pull(task_ids='make_MVP_prediction')
    p_dpoy = ti.xcom_pull(task_ids='make_DPOY_prediction')
    rookie_players, p_roy = ti.xcom_pull(task_ids='make_ROY_prediction')
    sm_players, p_smoy = ti.xcom_pull(task_ids='make_SMOY_prediction')
    
    player_names = df_players[['player']].values
    result = np.hstack((player_names, p_mvp))
    result = np.hstack((result, p_dpoy))
    
    rookie_names = rookie_players[['player']].values
    result = np.hstack((result, np.zeros(player_names.shape)))
    index = np.where(result[:,0] == rookie_names)[1]
    j=0
    for i in index:
        result[i][3] = p_roy[j][0]
        j += 1
        
    sm_names = sm_players[['player']].values
    result = np.hstack((result, np.zeros(player_names.shape)))
    index = np.where(result[:,0] == sm_names)[1]
    j=0
    for i in index:
        result[i][4] = p_smoy[j][0]
        j += 1
        
    # store the results in a csv file
    df = pd.DataFrame(result, columns = ['Player Name', 'p(MVP)', 'p(DPOY)', 'p(ROY)', 'p(SMOY)'])
    df['p(MVP)'] = df['p(MVP)'] * 100
    df['p(MVP)'] = df['p(MVP)'].astype(float).round(3)
    df['p(DPOY)'] = df['p(DPOY)'] * 100
    df['p(DPOY)'] = df['p(DPOY)'].astype(float).round(3)
    df['p(ROY)'] = df['p(ROY)'] * 100
    df['p(ROY)'] = df['p(ROY)'].astype(float).round(3)
    df['p(SMOY)'] = df['p(SMOY)'] * 100
    df['p(SMOY)'] = df['p(SMOY)'].astype(float).round(3)
    df.to_csv('~/airflow/dags/Player Results.csv', index=None)

############################################
# DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

default_args = {
    'owner': 'zhanshu',
    'depends_on_past': False,
    'email': ['zs2584@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'Player_Awards_Prediction',
    default_args=default_args,
    description='Player_Awards_Prediction',
    schedule_interval='* 7 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

##########################################
# DEFINE AIRFLOW OPERATORS
##########################################

    # t* examples of tasks created by instantiating operators
    
    fetch_new_player_data = PythonOperator(
        task_id='fetch_new_player_data',
        python_callable=fetch_all_new_data,
        provide_context = True,
    )
    
    fetch_new_rookie_data = PythonOperator(
        task_id='fetch_new_rookie_data',
        python_callable=fetch_rookie_new_data,
        provide_context = True,
    )
    
    fetch_new_substitute_data = PythonOperator(
        task_id='fetch_new_substitute_data',
        python_callable=fetch_sm_new_data,
        provide_context = True,
    )
    
    fetch_ROY_training_data = PythonOperator(
        task_id='fetch_ROY_training_data',
        python_callable=fetch_roy_training,
        provide_context = True,
    )
    
    fetch_SMOY_training_data = PythonOperator(
        task_id='fetch_SMOY_training_data',
        python_callable=fetch_smoy_training,
        provide_context = True,
    )
    
    fetch_MVP_training_data = PythonOperator(
        task_id='fetch_MVP_training_data',
        python_callable=fetch_mvp_training,
        provide_context = True,
    )
    
    fetch_DPOY_training_data = PythonOperator(
        task_id='fetch_DPOY_training_data',
        python_callable=fetch_dpoy_training,
        provide_context = True,
    )
    
    train_MVP_model = PythonOperator(
        task_id='train_MVP_model',
        python_callable=train_mvp,
        provide_context = True,
    )
    
    train_DPOY_model = PythonOperator(
        task_id='train_DPOY_model',
        python_callable=train_dpoy,
        provide_context = True,
    )
    
    train_SMOY_model = PythonOperator(
        task_id='train_SMOY_model',
        python_callable=train_smoy,
        provide_context = True,
    )
    
    train_ROY_model = PythonOperator(
        task_id='train_ROY_model',
        python_callable=train_roy,
        provide_context = True,
    )
    
    make_MVP_prediction = PythonOperator(
        task_id='make_MVP_prediction',
        python_callable=predict_mvp,
        provide_context = True,
    )
    
    make_DPOY_prediction = PythonOperator(
        task_id='make_DPOY_prediction',
        python_callable=predict_dpoy,
        provide_context = True,
    )
    
    make_ROY_prediction = PythonOperator(
        task_id='make_ROY_prediction',
        python_callable=predict_roy,
        provide_context = True,
    )
    
    make_SMOY_prediction = PythonOperator(
        task_id='make_SMOY_prediction',
        python_callable=predict_smoy,
        provide_context = True,
    )
    
    save_prediction_results = PythonOperator(
        task_id='save_prediction_results',
        python_callable=save_results,
        provide_context = True,
    )



##########################################
# DEFINE TASKS HIERARCHY
##########################################

    # task dependencies 
    fetch_MVP_training_data >> train_MVP_model
    fetch_DPOY_training_data >> train_DPOY_model
    fetch_ROY_training_data >> train_ROY_model
    fetch_SMOY_training_data >> train_SMOY_model
    [fetch_new_player_data, train_MVP_model] >> make_MVP_prediction
    [fetch_new_player_data, train_DPOY_model] >> make_DPOY_prediction
    [fetch_new_rookie_data, train_ROY_model] >> make_ROY_prediction
    [fetch_new_substitute_data, train_SMOY_model] >> make_SMOY_prediction
    [make_MVP_prediction, make_DPOY_prediction, make_ROY_prediction, make_SMOY_prediction] >> save_prediction_results




