
#update the current version
sudo apt update

#install java
sudo apt install openjdk-8-jdk -y

#verify java installation
java -version

#install http package
sudo apt install apt-transport-https

#add cassandra repo
sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 40x main" > /etc/apt/sources.list.d/cassandra.list'

wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

#update repo set
sudo apt update

#install cassandra
sudo apt install cassandra


#verify installation status
nodetool status
# or by entering
sudo systemctl status cassandra

# start , restart, stop cassandra
sudo systemctl start cassandra

sudo systemctl restart cassandra

sudo systemctl stop cassandra

# start automatically when booting
sudo systemctl enable cassandra

# get cassandra config
sudo cp /etc/cassandra/cassandra.yaml /etc/cassandra/cassandra.yaml.backup

sudo nano /etc/cassandra/cassandra.yaml


#All these commands are coming from 
# https://phoenixnap.com/kb/install-cassandra-on-ubuntu