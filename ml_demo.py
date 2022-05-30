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

#route prediction
def route_prediction(data):
    data['Prediction'] = 0
    data.loc[data['Total_Distance'] > data['Total_Distance'].shift(1), 'Prediction'] = 1
    data.dropna(inplace=True)
    return data

#function to find Longest Substring Without Repeating Characters
def find_longest_substring(string):
    max_length = 0
    start = 0
    end = 0
    for i in range(len(string)):
        for j in range(i+1, len(string)+1):
            substring = string[i:j]
            if len(substring) > max_length and is_unique(substring):
                max_length = len(substring)
                start = i
                end = j
    return string[start:end]

#function for Given the head of a linked list, reverse the nodes of the list k at a time, and return the modified list.
def reverse_k_nodes(head, k):
    current = head
    prev = None
    count = 0
    while current and count < k:
        next = current.next
        current.next = prev
        prev = current
        current = next
        count += 1
    if next:
        head.next = reverse_k_nodes(next, k)
    return prev

#function to convert integer to roman number
def int_to_roman(num):
    val = [
        1000, 900, 500, 400,
        100, 90, 50, 40,
        10, 9, 5, 4,
        1
    ]
    syb = [
        "M", "CM", "D", "CD",
        "C", "XC", "L", "XL",
        "X", "IX", "V", "IV",
        "I"
    ]
    roman_num = ''
    i = 0
    while  num > 0:
        for _ in range(num // val[i]):
            roman_num += syb[i]
            num -= val[i]
        i += 1
    return roman_num


#function to convert roman number to integer
def roman_to_int(s):
    rom_val = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    int_val = 0
    for i in range(len(s)):
        if i > 0 and rom_val[s[i]] > rom_val[s[i - 1]]:
            int_val += rom_val[s[i]] - 2 * rom_val[s[i - 1]]
        else:
            int_val += rom_val[s[i]]
    return int_val

#function to convert interger to hexadecimal
def int_to_hex(num):
    if num == 0:
        return '0'
    hex_val = {10: 'A', 11: 'B', 12: 'C', 13: 'D', 14: 'E', 15: 'F'}
    hex_num = ''
    while num > 0:
        if num % 16 > 9:
            hex_num = hex_val[num % 16] + hex_num
        else:
            hex_num = str(num % 16) + hex_num
        num = num // 16
    return hex_num







