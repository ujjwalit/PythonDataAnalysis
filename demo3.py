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

#replace garbage values


