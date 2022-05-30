import flask
from flask import Flask, render_template, request, redirect, url_for
import os
import sys
import json
import requests
import time

#function to initialize the app
def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'secret!'
    return app

#function to create the home page
@app.route('/')
def home():
    return render_template('index.html')

#function to create the about page
@app.route('/about')
def about():
    return render_template('about.html')

#function to return the data
@app.route('/data')
def data():
    return render_template('data.html')

#function for login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'admin':
            return redirect(url_for('data'))
        else:
            return redirect(url_for('home'))
    return render_template('login.html')

#function for submit button
@app.route('/submit', methods=['GET', 'POST'])
def submit():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'admin' and password == 'admin':
            return redirect(url_for('data'))
        else:
            return redirect(url_for('home'))
    return render_template('submit.html')







#function to run the app
if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)

#function to run the app on port 5000
if __name__ == '__main__':
    app = create_app()
    app.run(host='localhost', port=5000)

