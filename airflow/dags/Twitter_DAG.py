from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.cassandra_hook import CassandraHook
import pandas as pd

from airflow.models import Variable

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra_sigv4.auth import SigV4AuthProvider

import tweepy
import json

consumer_key = Variable.get("consumer_key")
consumer_secret = Variable.get("consumer_secret")
access_token = Variable.get("access_token")
access_token_secret = Variable.get("access_token_secret")
state= 'Valid'

def put_into_cassandra(data_frame, table_name):
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('twitter_ks')
    columns = " "
    column_count = " "
    for column_name in data_frame.columns:
        formatted = (str(column_name) + ',').replace('.', '_')
        columns = columns + formatted
        column_count = column_count + "?,"
    columns = columns[:-1]
    column_count = column_count[:-1]
    query = "INSERT INTO "+table_name+"("+columns+") VALUES ("+column_count+")"
    prepared = session.prepare(query)

    for item in data_frame.iterrows():
        row = []
        for cell in item[1]:
            row.append(str(cell))
        session.execute(prepared, row)

def get_tweets(username):

    # Authorization to consumer key and consumer secret
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    # Access to user's access key and access secret
    auth.set_access_token(access_token, access_token_secret)

    # Calling api
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name=username)

    # create array of tweet information: username,
    # tweet id, date/time, text
    tweets_for_csv = [tweet.text for tweet in tweets] # CSV file created
    tweet_series = pd.Series(tweets_for_csv)
    
    return tweet_series

def fetch_followed_tweets(state, ti):
    users_list=ti.xcom_pull(key='users_list', task_ids='Fetch_users_from_Twitter')
    for user in users_list:
        frame = { 'screen_name': user, 'tweet': get_tweets(user) }
        tweets_df = pd.DataFrame(frame)
        print(tweets_df)
        put_into_cassandra(tweets_df, 'dim_tweets')

def get_users(state, ti):

    # Authorization to consumer key and consumer secret
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    # Access to user's access key and access secret
    auth.set_access_token(access_token, access_token_secret)

    # Calling api
    api = tweepy.API(auth)
    users = api.search_users(100)
    
    users_for_json = [user._json for user in users] # CSV file created
    users_df = pd.json_normalize(users_for_json)

    users_list = users_df['screen_name'].to_list()
    put_into_cassandra(users_df, 'dim_users')

    ti.xcom_push(key='users_list', value=users_list)

    #return users_df

# Define the default dag arguments.
default_args = {
		'owner' : 'Abhijit Patra',
		'depends_on_past' :False,
		'email' :['abhijit.patra@affine.ai'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=1)
		}


# Define the dag, the start date and how frequently it runs.
dag = DAG(
		dag_id='DAG_Twitter_ETL',
		default_args=default_args,
		start_date=datetime(2022,1,1),
		schedule_interval=None,
        catchup=False
        )

Fetch_users = PythonOperator(
    task_id='Fetch_users_from_Twitter',
    python_callable=get_users,
    provide_context=True,
    op_kwargs={
        'state':state
    },
    dag=dag
)

Fetch_tweets = PythonOperator(
    task_id='Fetch_tweets_from_Twitter',
    python_callable=fetch_followed_tweets,
    provide_context=True,
    op_kwargs={
        'state':state
    },
    dag=dag
)

Fetch_users >> Fetch_tweets