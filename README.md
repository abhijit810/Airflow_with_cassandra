# Airflow_with_cassandra

## Steps to run:
### Cassandra Database
#### 1. Set up cassandra DB -  to do this, run all the commands in file cassandra/install-cassandra
#### 2. create the respective keyspace and tables from scripts given in file cassandra/cassandra_cqls.cql

### install required libraries using the command below
#### pip3 install -r requirements.txt

### create a user in airflow:
#### airflow users create \
####  --username airflow \
####  --firstname Iron \
####  --lastname man \
####  --role Admin \
####  --email example.email@your.org

### run airflow standalone using the below command:
#### sh airflow_setup.sh

### create these 4 airflow variables to store your credentials, these keys will be given tou you by creating a dev API twitter account
##### consumer_key
##### consumer_secret
##### access_token
##### access_token_secret

### finally
#### open the URL "http://localhost:8080/home"
#### login with username and password you just created
#### you should see a paused DAG "DAG_Twitter_ETL" along with other example dags.
#### Unpause this DAG, Run the same and verify the results by checking the respective tables in database.

### verify in DB
#### in terminal run "cqlsh"
#### then run "use twitter_ks;"
#### then do select * from dim_users;
#### then do select * from dim_tweets;