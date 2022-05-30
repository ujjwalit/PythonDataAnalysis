from datetime import datetime as dt
import pandas as pd


def duplicate_word_count(file_name):
    """
    Count the number of duplicate words in a file.
    """
    with open(file_name) as f:
        words = f.read().split()
    return len(set(words))

# Read the file
print(duplicate_word_count('README.md'))

# find duplicates in the tables and remove them
df = pd.read_csv('data/tables/table_1.csv')
df.drop_duplicates(inplace=True)
df.to_csv('data/tables/table_1.csv', index=False)

# remove ascii characters from column names
df = pd.read_csv('data/tables/table_2.csv')
df.columns = [x.encode('ascii', 'ignore').decode('ascii') for x in df.columns]
df.to_csv('data/tables/table_2.csv', index=False)

# fill null values in column with NaN
df = pd.read_csv('data/tables/table_3.csv')
df.fillna(value=pd.np.nan, inplace=True)
df.to_csv('data/tables/table_3.csv', index=False)

# create new column having running total of sales for each product and plot in a bar chart
df = pd.read_csv('data/tables/table_4.csv')
df['Total Sales'] = df.groupby('Product')['Sales'].cumsum()
df.plot(kind='bar', x='Product', y='Total Sales')

# function to create new column and store week from date column
def create_week_column(df):
    df['Week'] = df['Date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').isocalendar()[1])
    return df

# function to check if a string is palindrome
def is_palindrome(s):
    return s == s[::-1]

# remove new line characters from column
df = pd.read_csv('data/tables/table_5.csv')
df['Text'] = df['Text'].str.replace('\n', ' ')
df.to_csv('data/tables/table_5.csv', index=False)

# remove space characters from column names
df = pd.read_csv('data/tables/table_6.csv')
df.columns = [x.replace(' ', '_') for x in df.columns]
df.to_csv('data/tables/table_6.csv', index=False)

# get only unique values from column
df = pd.read_csv('data/tables/table_7.csv')
df['Unique'] = df['Unique'].unique()

# join two dataframes on a column and drop duplicates
df1 = pd.read_csv('data/tables/table_8.csv')
df2 = pd.read_csv('data/tables/table_9.csv')
df = df1.merge(df2, on='ID')
df.drop_duplicates(inplace=True)

from datetime import datetime
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator  # noqa
##import connection.redshift as conn_module
from airflow import AirflowException
import slackNotification.slack as slackAlert

dag = DAG(
    'dag_name',
    description='description',
    schedule_interval='0 0 * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': [' '],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'priority_weight': 10,
        'pool': 'default_pool',
        'queue': 'default_queue',
        'sla': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=5),
        'on_failure_callback': None,
        'on_success_callback': None,
        'on_retry_callback': None,
        'trigger_rule': 'all_success',
        'resources': None
    }
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

bash_op = BashOperator(
    task_id='bash_op',
    bash_command='echo "Hello World"',
    dag=dag
)

# create a function to find max sales for each product
def max_sales(df):
    df['Max Sales'] = df.groupby('Product')['Sales'].transform('max')
    return df


# remove duplicates from table
def remove_duplicates(df):
    df.drop_duplicates(inplace=True)
    return df

start_time = dt.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

import logging
import os

class log_util:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path

    def get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(self.log_file_path)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.basicConfig(filename=self.log_file_path, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        return logger

import boto3
s3 = boto3.resource('s3')
inbucket = s3.Bucket('inbucket')
outbucket = s3.Bucket('outbucket')
for obj in inbucket.objects.filter(Prefix='data/tables/'):
    #print(obj.key)
    obj.download_file(obj.key)
    print(obj.key)
    body = obj.get()['Body'].read()

#function to upload csv to s3
def upload_to_s3(file_name, bucket_name):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket_name, file_name)    

#function to read excel file first sheet    
def read_excel_file(file_name):
    data = pd.read_excel(file_name, sheet_name=0)
    return data

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = pyspark.SparkContext()
spark = SparkSession(sc)

df = spark.read.csv('data/tables/table_1.csv', header=True, inferSchema=True)
df.show()

df1 = spark.read.csv('data/tables/table_2.csv', header=True, inferSchema=True)
df1.show()

df.CreateTempView("df")
df1.CreateTempView("df1")

df2 = spark.sql("select * from df where df.id = df1.id")
df2.show()

df1.join(df2, df1.id == df2.id).show()

df.withColumn("new_col", lit("new_val")).show()
df.column = df.column.cast(pd.StringDtype())
df.printSchema()

#remove duplicates from dataframe
df.drop_duplicates(inplace=True)

#read delta table
df = spark.read.format("delta").load("s3://delta-bucket/delta-table")

#read parquet table
df = spark.read.parquet("s3://parquet-bucket/parquet-table")

#read streaming data from kafka topic
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic").load()

#read data from bigquery
df = spark.read.format("bigquery").option("project", "project-id").option("table", "dataset.table").load()

#read data from mongo db
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost/db.collection").load()

df = pd.read_csv('data/tables/table_1.csv', header=True)

#read data from gcs using pandas
df = pd.read_csv('gs://bucket-name/file-name.csv', header=True)

#create class for spark job
class spark_job:
    def __init__(self, spark_session, spark_job_name):
        self.spark_session = spark_session
        self.spark_job_name = spark_job_name
        self.spark_job_logger = log_util('spark_jobs/' + spark_job_name + '.log').get_logger()
        self.spark_job_logger.info('Spark Job Started')

    def run(self):
        try:
            self.spark_job_logger.info('Spark Job Started')
            self.spark_job_logger.info('Spark Job Completed')
        except Exception as e:
            self.spark_job_logger.error('Spark Job Failed')
            self.spark_job_logger.error(e)

#function to authenticate using OAuth2 authentication
def authenticate_using_oauth2(client_id, client_secret, refresh_token):
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        'service_account_key.json',
        scopes=['https://www.googleapis.com/auth/cloud-platform'],

    )
    credentials.refresh(Request())
    return credentials

#function to authenticate using GCP service account
def authenticate_using_gcp_service_account(service_account_key_file):
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        service_account_key_file,
        scopes=['https://www.googleapis.com/auth/cloud-platform'],
        target_audience=None,
    )
    credentials.refresh(Request())
    return credentials

#function to validate json file
def validate_json_file(json_file_path):
    import json
    with open(json_file_path) as json_file:
        try:
            json.load(json_file)
        except ValueError as e:
            return False
        return True

#function to run flask app
def run_flask_app(app, port):
    app.run(host='localhost', port=port)

#check if file exists
def file_exists(file_path):
    import os.path
    return os.path.isfile(file_path)

#check if a port is available
def is_port_available(port):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('', port))
        s.close()
        return True
    except socket.error:
        return False

#cloudformation template
def create_cloudformation_template(template_file_path, template_name):
    import json
    with open(template_file_path) as template_file:
        template = json.load(template_file)
    template['AWSTemplateFormatVersion'] = '2010-09-09'
    template['Description'] = 'AWS CloudFormation template to deploy a Cloud Function'
    template['Outputs'] = {
        'FunctionName': {
            'Value': {
                'Fn::GetAtt': [
                    template_name,
                    'Name'
                ]
            }
        },
        'FunctionArn': {
            'Value': {
                'Fn::GetAtt': [
                    template_name,
                    'Arn'
                ]
            }
        },
        'FunctionVersion': {
            'Value': {
                'Fn::GetAtt': [
                    template_name,
                    'Version'
                ]
            }
        }
    }
    return template


































    

    











