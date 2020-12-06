#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 01:59:33 2020

@author: maurer
"""

import dash
import flask

server = flask.Flask(__name__)
app = dash.Dash(__name__, suppress_callback_exceptions=True, meta_tags=[{"name": "viewport", "content": "width=device-width"}], server = server)
appname = "Nowum Energy Monitor"
app.title = appname

