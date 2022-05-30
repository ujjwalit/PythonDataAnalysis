!/bin/bash

#check if the user is root
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

#run script as root
sudo ./demo.sh

#install dependencies
apt-get install -y python-pip python-dev build-essential

#install apache2
apt-get install -y apache2

#configure apache2
cp /etc/apache2/apache2.conf /etc/apache2/apache2.conf.bak
cp /etc/apache2/sites-available/000-default.conf /etc/apache2/sites-available/000-default.conf.bak

#add ip address to hosts file
echo "localhost" >> /etc/hosts

#change user and group
sed -i 's/www-data/vagrant/g' /etc/apache2/envvars

#copy file from local to hdfs
hdfs dfs -copyFromLocal /vagrant/demo.py /user/vagrant/demo.py

#copy file from local to s3
aws s3 cp /vagrant/demo.py s3://demo-bucket/demo.py

#copy file from s3 to hdfs
hdfs dfs -copyFromLocal s3://demo-bucket/demo.py /user/vagrant/demo.py

#copy file from local to gcs
gsutil cp /vagrant/demo.py gs://demo-bucket/demo.py

#configure aws credentials
aws configure set aws_access_key_id AKIAJZQZQZQZQZQZQZQ









