#!/bin/bash
sudo apt install -y libpq-dev python2-dev
virtualenv -p /usr/bin/python2 venv
source venv/bin/activate
pip install -r requirements.txt
