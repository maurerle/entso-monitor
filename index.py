#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 01:58:15 2020

@author: maurer
"""

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from app import app, server
import entsoe_dash
import entsog_dash

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    print(pathname)
    if pathname == '/entsoe':
        pass
        #return entsoe_dash.layout
    elif pathname == '/entsog':
        return entsog_dash.layout
    elif pathname == '/':
        return dcc.Location(pathname="/entsog", id="entsog_redirect", refresh=True)
    else:
        return '404'

if __name__ == '__main__':
    app.run_server(debug=True)
