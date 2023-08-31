from datetime import datetime, timedelta
from textwrap import dedent
import time
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn import preprocessing
import requests
import lxml

from nba_api.stats.endpoints import teamyearbyyearstats
from nba_api.stats.static import teams

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

def fetch_new_data(**kwargs):
    team_ids = []
    team_names = []
    team_info = teams.get_teams()
    for i in range(len(team_info)):
        team_ids.append(team_info[i]['id'])
        team_names.append(team_info[i]['full_name'])
    df_teams = None
    for i in range(len(team_ids)):
        t_id = team_ids[i]
        team_stats = teamyearbyyearstats.TeamYearByYearStats(t_id)
        df = team_stats.get_data_frames()[0]
        df = df[df['YEAR'] == '2022-23']
        df_teams = pd.concat([df_teams, df])
    
    # save team stats to a csv file
    df_teams['Team Name'] = team_names
    df_stats = df_teams[['Team Name', 'GP', 'WINS', 'LOSSES', 'WIN_PCT', 'CONF_RANK', 'DIV_RANK']]
    df_stats.to_csv('~/airflow/dags/2023 Team.csv', index=None)
    
    # preprocess new data
    df_teams['orb_pg'] = df_teams['OREB']/df_teams['GP']
    df_teams['drb_pg'] = df_teams['DREB']/df_teams['GP']
    df_teams['ast_pg'] = df_teams['AST']/df_teams['GP']
    df_teams['stl_pg'] = df_teams['STL']/df_teams['GP']
    df_teams['blk_pg'] = df_teams['BLK']/df_teams['GP']
    df_teams['tov_pg'] = df_teams['TOV']/df_teams['GP']
    df_teams['pts_pg'] = df_teams['PTS']/df_teams['GP']
    return team_names, df_teams

def fetch_training_data(**kwargs):
    # read training data and preprocess
    team_data = pd.read_csv("~/airflow/dags/Team Totals.csv")
    team_data = team_data[team_data['season'] >= 1980]
    team_data = team_data[team_data['season'] != 2023]
    team_data = team_data[team_data['team'] != 'League Average'].reset_index(drop=True)
    team_data['orb_pg'] = team_data['orb']/team_data['g']
    team_data['drb_pg'] = team_data['drb']/team_data['g']
    team_data['ast_pg'] = team_data['ast']/team_data['g']
    team_data['stl_pg'] = team_data['stl']/team_data['g']
    team_data['blk_pg'] = team_data['blk']/team_data['g']
    team_data['tov_pg'] = team_data['tov']/team_data['g']
    team_data['pts_pg'] = team_data['pts']/team_data['g']
    return team_data
    
def train_model(**kwargs):
    ti = kwargs['ti']
    train_data = ti.xcom_pull(task_ids='fetch_team_training_data')
    X_train = train_data[["fg", "fg_percent", "x3p", "x3p_percent", "ft", "ft_percent", "orb", "orb_pg", "drb", "drb_pg", "ast", "ast_pg", "stl", "stl_pg", "blk", "blk_pg", "tov", "tov_pg", "pts", "pts_pg"]].values
    y_train = train_data["playoffs"].values
    # standardize training data
    X_train = preprocessing.StandardScaler().fit_transform(X_train)
    
    lr = LogisticRegression(random_state=0).fit(X_train, y_train)
    return lr

def make_predictions(**kwargs):
    ti = kwargs['ti']
    team_names, df_teams = ti.xcom_pull(task_ids='fetch_new_team_data')
    lr = ti.xcom_pull(task_ids='train_PLAYOFFS_model')
    
    test_data = df_teams[["FGM", "FG_PCT", "FG3M", "FG3_PCT", "FTM", "FT_PCT", "OREB", "orb_pg", "DREB", "drb_pg", "AST", "ast_pg", "STL", "stl_pg", "BLK", "blk_pg", "TOV", "tov_pg", "PTS", "pts_pg"]].values
    test_data = preprocessing.StandardScaler().fit_transform(test_data)
    predictions = lr.predict_proba(test_data)
    p_arr = predictions[:,1].reshape(-1,1)
    
    team_names = team_names.reshape(-1,1)
    team_names = np.array(team_names)
    
    return team_names, p_arr
    
def save_results(**kwargs):
    ti = kwargs['ti']
    team_names, p_arr = ti.xcom_pull(task_ids='make_PLAYOFFS_prediction')
    result = np.hstack((team_names, p_arr))
    df = pd.DataFrame(result, columns = ['Team Name', 'p(Playoffs)'])
    df['p(Playoffs)'] = df['p(Playoffs)'] * 100
    df['p(Playoffs)'] = df['p(Playoffs)'].astype(float).round(3)
    df.to_csv('~/airflow/dags/Team Results.csv', index=None)


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
    'Team_Performance_Prediction',
    default_args=default_args,
    description='Team_Performance_Prediction',
    schedule_interval='* 7 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

##########################################
# DEFINE AIRFLOW OPERATORS
##########################################

    # t* examples of tasks created by instantiating operators
    
    fetch_new_team_data = PythonOperator(
        task_id='fetch_new_team_data',
        python_callable=fetch_new_data,
        provide_context = True,
    )
    
    fetch_team_training_data = PythonOperator(
        task_id='fetch_team_training_data',
        python_callable=fetch_training_data,
        provide_context = True,
    )
    
    train_PLAYOFFS_model = PythonOperator(
        task_id='train_PLAYOFFS_model',
        python_callable=train_model,
        provide_context = True,
    )
    
    make_PLAYOFFS_prediction = PythonOperator(
        task_id='make_PLAYOFFS_prediction',
        python_callable=make_predictions,
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
    fetch_team_training_data >> train_PLAYOFFS_model
    [fetch_new_team_data, train_PLAYOFFS_model] >> make_PLAYOFFS_prediction
    make_PLAYOFFS_prediction >> save_prediction_results
    




