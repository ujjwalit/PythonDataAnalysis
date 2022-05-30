import os
import sys
import time
import json
import requests
import subprocess
import threading
import signal
import argparse
import logging

# Global variables
# ------------------------------------------------------------------------------
# The following variables are used to store the configuration of the test
# environment.
# ------------------------------------------------------------------------------

#check if file exists
def file_exists(file_path):
    try:
        with open(file_path) as f:
            return True
    except FileNotFoundError:
        return False

#move file to directory
def move_file(file_path, directory):
    try:
        os.rename(file_path, directory)
    except FileNotFoundError:
        print("File not found")

#get file name
def get_file_name(file_path):
    return os.path.basename(file_path)

#read file
def read_file(file_path):
    with open(file_path) as f:
        return f.read()

#search for string in file
def search_file(file_path, string):
    if file_exists(file_path):
        with open(file_path) as f:
            if string in f.read():
                return True
            else:
                return False
    else:
        return False

#check last 10 days modified file
def check_last_modified(file_path):
    if file_exists(file_path):
        modified_time = os.path.getmtime(file_path)
        if time.time() - modified_time < 86400 * 10:
            return True
        else:
            return False
    else:
        return False

#download data from sftp
def download_data(file_path, directory):
    try:
        os.mkdir(directory)
    except FileExistsError:
        pass
    try:
        subprocess.run(["sftp", "-b", file_path, "sftp://sftp.scc.kit.edu/"])
    except FileNotFoundError:
        print("File not found")

