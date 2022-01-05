from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
from airflow.contrib.hooks.cassandra_hook import CassandraHook
import json
import pandas as pd
import numpy as np

from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider

def import_data():
	#cluster = Cluster(['15.207.98.101'])

	ssl_context = SSLContext(PROTOCOL_TLSv1_2)
	ssl_context.load_verify_locations('sf-class2-root.crt')
	ssl_context.verify_mode = CERT_REQUIRED

	# use this if you want to use Boto to set the session parameters.
	boto_session = boto3.Session(aws_access_key_id="AKIAYOTRUKBAYOMI5OCU",
                             aws_secret_access_key="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",
                             region_name="ap-south-1")
	auth_provider = SigV4AuthProvider(boto_session)

	#cluster = Cluster(['cassandra.ap-south-1.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
	cluster = Cluster()
	session = cluster.connect('AbhijitKeySpace')
	session.set_keyspace('AbhijitKeySpace')

	def pandas_factory(colnames, rows):
		return pd.DataFrame(rows, columns=colnames)

	session.row_factory = pandas_factory
	session.default_fetch_size = None

	query = "SELECT * FROM employee_dimension;"
	rslt = session.execute(query, timeout=None)
	data = rslt._current_rows
	return data


def load_data(ds, **kwargs):
	cluster = Cluster()
	session = cluster.connect('cluster_test')
	session.set_keyspace('cluster_test')
	
	valid_rows = (data.notnull().all(1))

	query_1 = "INSERT INTO emp_valid_data (emp_id,emp_name,emp_dob,emp_city,emp_zip,emp_email) VALUES (?,?,?,?,?,?)"
	prepared_1 = session.prepare(query_1)
	query_2 = "INSERT INTO emp_invalid_data (emp_id,emp_name,emp_dob,emp_city,emp_zip,emp_email) VALUES (?,?,?,?,?,?)"
	prepared_2 = session.prepare(query_2)
	for row, values in enumerate(data.notnull().all(1)):
		if values is True:
			session.execute(prepared_1, (data[:row]['emp_id'],data[:row]['emp_name'],data[:row]['emp_dob'],data[:row]['emp_city'],data[:row]['emp_zip'],data[:row]['emp_email']))
		else:
			session.execute(prepared_2, (data[:row]['emp_id'],data[:row]['emp_name'],data[:row]['emp_dob'],data[:row]['emp_city'],data[:row]['emp_zip'],data[:row]['emp_email']))
		#session.execute(prepared_2, (data[:row]['emp_id'],data[:row]['emp_name'],data[:row]['emp_dob'],data[:row]['emp_city'],data[:row]['emp_zip'],data[:row]['emp_email']))

# Define the default dag arguments.
default_args = {
		'owner' : 'Airflow',
		'depends_on_past' :False,
		'email' :['ivypro@gmail.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=1)
		}


# Define the dag, the start date and how frequently it runs.
dag = DAG(
		dag_id='cassandra_read_write',
		default_args=default_args,
		start_date=datetime(2022,1,1),
		schedule_interval=None,
        catchup=False
)

# First task is to query get the data from Cassandra.
get_data = PythonOperator(
			task_id='get_data',
			python_callable=import_data,
			dag=dag)


# Second task is to process the data and load into the database.
transform_load =  PythonOperator(
			task_id='transform_load',
			provide_context=True,
			python_callable=load_data,
			dag=dag)

# Set get_data "upstream" of transform_load, i.e. get_data must be completed
# before transform_load can be started.
get_data >> transform_load 