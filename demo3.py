from os import rename
import pandas as pd
import numpy as np

#read data
df = pd.read_csv('data.csv')

#clean data
df = df.dropna()

#create a new column
df['new_column'] = df['column1'] + df['column2']

#remove a column
df = df.drop('column1', axis=1)

#rename columns
df = df.rename(columns={'column2':'new_column'})

#run a query
df = df[df['column1'] > 0]

#change delimiter
df = pd.read_csv('data.csv', sep=';')

#read only specific columns
df = pd.read_csv('data.csv', usecols=['column1', 'column2'])

#select only last 100 rows
df = df.tail(100)

#parse dates in a column to datetime
df['date'] = pd.to_datetime(df['date'])

#check if a column exists
df.columns.contains('column1')

#check if a column has null values
df.isnull().any()

#function to read data line by line and apply a function to each line
def read_data(file_path, func):
    with open(file_path) as f:
        for line in f:
            func(line)

#function to read excel from s3 using boto3
def read_excel(file_path):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    df = pd.read_excel(obj)
    return df

#function to trigger a lambda function
def trigger_lambda(func_name, payload):
    import boto3
    client = boto3.client('lambda')
    response = client.invoke(FunctionName=func_name, InvocationType='Event', Payload=payload)
    return response

#function to trigger a glue job
def trigger_glue_job(job_name, payload):
    import boto3
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=job_name, Arguments=payload)
    return response

#function to create glue job
def create_glue_job(job_name, payload):
    import boto3
    glue = boto3.client('glue')
    response = glue.create_job(Name=job_name, Role='arn:aws:iam::123456789012:role/glue_role', Command={'Name': 'glueetl', 'ScriptLocation': 's3://bucket_name/script_location'}, DefaultArguments=payload)
    return response

#lambda function to read data from s3
def read_data_from_s3(event, context):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    df = pd.read_csv(obj)
    return df

#lambda function to convert csv to parquet
def convert_csv_to_parquet(event, context):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    df = pd.read_csv(obj)
    df.to_parquet('s3://bucket_name/file_name.parquet')
    return df

#fuction to parse apache logs
def parse_apache_logs(event, context):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    df = pd.read_csv(obj, sep=' ', header=None, names=['ip', 'date', 'time', 'method', 'url', 'status', 'size'])
    df['date'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df = df.drop(['time', 'method', 'url', 'status', 'size'], axis=1)
    df = df.dropna()
    return df

#function to list last week modified files in s3
def list_last_week_modified_files(event, context):
    import boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('bucket_name')
    response = bucket.objects.filter(Prefix='folder_name/', ModifiedSince=datetime.now() - timedelta(days=7))
    return response

#function to check if a file exists in s3
def check_if_file_exists(event, context):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    response = obj.exists()
    return response

#function to remove a file from s3
def remove_file_from_s3(event, context):
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object('bucket_name', 'file_name')
    response = obj.delete()
    return response

#start a thread
def start_thread(func):
    import threading
    thread = threading.Thread(target=func)
    thread.start()
    return thread

#read files in parallel
def read_files_in_parallel(file_paths, func):
    import threading
    threads = []
    for file_path in file_paths:
        thread = threading.Thread(target=func, args=(file_path,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

#read large files in chunks
def read_large_file_in_chunks(file_path, chunk_size):
    import io
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data

#spark broadcast variable
def broadcast_variable(value):
    import pickle
    from pyspark.broadcast import Broadcast
    return Broadcast(pickle.dumps(value))

#spark repartition
def repartition(df, num_partitions):
    return df.repartition(num_partitions)

#colease dataframes
def coalesce(dfs, num_partitions):
    return dfs.coalesce(num_partitions)

#write dataframe to parquet with compression and partition by date
def write_dataframe_to_parquet(df, file_path, compression, partition_by):
    df.write.parquet(file_path, compression=compression, partitionBy=partition_by)

#convert a dataframe to a dictionary
def convert_dataframe_to_dict(df):
    return df.to_dict('records')

#spark streaming context
def create_streaming_context(batch_interval):
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    ssc = pyspark.streaming.StreamingContext(sc, batch_interval)
    return ssc

#start a spark streaming context
def start_streaming_context(ssc):
    ssc.start()
    return ssc

#read streaming data from kafka
def read_streaming_data_from_kafka(ssc, topic, kafka_broker_url):
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    kafka_broker_url = kafka_broker_url.split(',')
    kafka_broker_url = [x.split(':') for x in kafka_broker_url]
    kafka_broker_url = [{'host': x[0], 'port': int(x[1])} for x in kafka_broker_url]
    kafka_broker_url = [{'metadata.broker.list': json.dumps(kafka_broker_url)}]
    df = spark.readStream.format('kafka').options(kafka_broker_url).option('subscribe', topic).load()
    return df


#spark window function
def create_window_function(window_duration, slide_duration):
    import pyspark
    from pyspark.sql import Window
    window = Window.partitionBy('key').orderBy('timestamp').rowsBetween(window_duration, slide_duration)
    return window

#create dataframe pivot table
def create_dataframe_pivot_table(df, columns, values, aggfunc='mean'):
    import pyspark
    from pyspark.sql import functions as F
    df = df.groupby(columns).agg(F.collect_list(values).alias(aggfunc))
    return df

#convert rows to columns
def convert_rows_to_columns(df):
    import pyspark
    from pyspark.sql import functions as F
    df = df.select(F.explode('data').alias('data'))
    return df

#error handling for spark
def handle_spark_exception(func):
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pyspark.sql.utils.AnalysisException as e:
            sc.stop()
            raise e
    return wrapper

#connect to redshift using psycopg2
def connect_to_redshift(host, port, dbname, user, password):
    import psycopg2
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    return conn

#execute query on redshift
def execute_query_on_redshift(conn, query):
    import psycopg2
    cur = conn.cursor()
    cur.execute(query)
    return cur

#check stl_load_error table for a record
def check_stl_load_error_table(conn, table_name, file_name, error_message):
    import psycopg2
    cur = conn.cursor()
    cur.execute("select count(*) from {} where file_name = '{}' and error_message = '{}'".format(table_name, file_name, error_message))
    return cur.fetchone()[0]























