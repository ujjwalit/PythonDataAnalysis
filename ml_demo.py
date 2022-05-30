import imp


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sklearn
import tensorflow as tf

data = pd.read_csv('data/data.csv')

test_set = data.sample(frac=0.2, random_state=1)
train_set = data.drop(test_set.index)

data.drop_duplicates(inplace=True)

#function to get the correlation matrix
def get_corr_matrix(data):
    corr_matrix = data.corr()
    return corr_matrix

#function to get the correlation matrix heatmap
def get_corr_heatmap(data):
    corr_matrix = get_corr_matrix(data)
    plt.figure(figsize=(10,8))
    sns.heatmap(corr_matrix, vmax=1, square=True)
    plt.show()

#function to create time series plot
def get_time_series_plot(data):
    plt.figure(figsize=(10,8))
    plt.plot(data['Date'], data['Close'])
    plt.xticks(rotation=45)
    plt.show()

#lstm model
def lstm_model(train, test, lstm_units=4, epochs=100):
    model = tf.keras.models.Sequential()
    model.add(tf.keras.layers.LSTM(units=lstm_units, input_shape=(train.shape[1], train.shape[2])))
    model.add(tf.keras.layers.Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(train, test, epochs=epochs, batch_size=1, verbose=2)
    return model

#function to get the predicted values
def get_predictions(model, data):
    x_input = data[len(data) - len(test_set) - 1:].values
    x_input = x_input.reshape((1, x_input.shape[0], x_input.shape[1]))
    y_pred = model.predict(x_input)
    y_pred = y_pred.reshape((y_pred.shape[1]))
    return y_pred

#train the model
def train_model(train, test, lstm_units=4, epochs=100):
    model = lstm_model(train, test, lstm_units, epochs)
    return model

#forecast the values
def forecast_values(model, data):
    y_pred = get_predictions(model, data)
    return y_pred

#fraud detection
def detect_fraud(data):
    data['Prediction'] = 0
    data.loc[data['Close'] > data['Close'].shift(1), 'Prediction'] = 1
    data.dropna(inplace=True)
    return data

#load public dataset for moview review sentiment analysis
def load_movie_review_data():
    data = pd.read_csv('data/movie_reviews.csv')
    return data

#function for deep learning model
def deep_learning_model(train, test, epochs=100):
    model = tf.keras.models.Sequential()
    model.add(tf.keras.layers.Dense(16, input_shape=(train.shape[1],)))
    model.add(tf.keras.layers.Dense(16))
    model.add(tf.keras.layers.Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(train, test, epochs=epochs, batch_size=1, verbose=2)
    return model





