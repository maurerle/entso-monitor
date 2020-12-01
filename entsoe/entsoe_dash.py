#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 23:31:45 2020

@author: maurer
"""

import dash
from datetime import datetime, date
import pandas as pd
from dash.dependencies import Input, Output, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html

from entsoe_data_manager import Filter
from entsoe_parquet_manager import EntsoeParquet
from entsoe_sqlite_manager import EntsoeSQLite
import plotly.express as px 
import json
import copy

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

import findspark
import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,SQLContext
findspark.init()

if True:
    dm= EntsoeSQLite('entsoe.db')    
else:
    try:
        spark
        print('using existing spark object')
    except:
        print('creating new spark object')
        conf = SparkConf().setAppName('entsoe').setMaster('local')
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    dm= EntsoeParquet('data',spark)
    
d = dm.powersystems('')

app.layout = html.Div(
    [
        dcc.Store(id="aggregate_data"),
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("fh-aachen.png"),
                            id="plotly-image",
                            style={
                                "height": "60px",
                                "width": "auto",
                                "margin-bottom": "25px",
                                "backgroundColor":'white',
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "ENTSO-E Energy Monitor",
                                    style={"margin-bottom": "0px"},
                                ),
                                html.H5(
                                    "Production Overview", style={"margin-top": "0px"}
                                ),
                            ]
                        )
                    ],
                    className="one-half column",
                    id="title",
                ),
            ],
            id="header",    
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
                    [dcc.Graph(id='choro-graph')],
                    id="locationMapContainer",
                    className="pretty_container",
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P(
                            "Select Time Filter:",
                            className="control_label",
                        ),
                        #dcc.Dropdown(options=[{'label':x, 'value': x} for x in range(2015, 2020)]),
                        dcc.DatePickerRange(
                            id='date_picker',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date(2020, 10, 19),
                            start_date=date(2020,2,21),
                            end_date=date(2020,3,4),
                            display_format='MMMM Y, DD',
                            #with_portal=True,
                            initial_visible_month='2020-02-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Country:", className="control_label"),
                        dcc.Dropdown(id="country_control",
                                     options=[{'label':x, 'value': x} for x in dm.countries()],
                                     value='DE',
                                     className="dcc_control",
                                    ),
                        html.P("Group by:", className="control_label"),
                        dcc.RadioItems(
                            id="group_by_control",
                            options=[
                                {"label": "Year", "value": "year"},
                                {"label": "Month", "value": "month"},
                                {"label": "Day ", "value": "day"},
                                {"label": "Hour", "value": "hour"},
                                {"label": "Minute", "value": "minute"},
                            ],
                            value="day",
                            labelStyle={"display": "inline-block"},
                            className="dcc_control",
                        ),
                        html.P("Energy generation type:", className="control_label"),
                        dcc.Dropdown(
                            id="energy_type_selector",
                            options=[
                                {"label": "Renewable ", "value": "renewable"},
                                {"label": "Traditional", "value": "traditional"},
                                {"label": "Nuclear", "value": "nuclear"},
                            ],
                            multi=True,
                            value=['renewable'],
                            className="dcc_control",
                        ),
                    ],
                    className="pretty_container four columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="capacity_graph")],
                            id="capacityGraphContainer",
                            className="pretty_container",
                        ),                        
                    ],
                    id="right-column",
                    className="eight columns",
                ),                
            ],
            className="row flex-display",
        ),
        html.Div(
                            [dcc.Graph(id="load_graph")],
                            id="loadGraphContainer",
                            className="pretty_container",
        ),
        html.Div(
                    [dcc.Graph(id="generation_graph")],
                    id="generationGraphContainer",
                    className="pretty_container",
        ),
])


layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="Satellite Overview",
    mapbox=dict(
        style="light",
        center=dict(lon=-78.05, lat=42.54),
        zoom=7,
    ),
)

app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("load_graph", "figure")],
)

with open("europe.geo.json", "r", encoding="utf-8") as f:
    geo = json.load(f)    

#geo['features'][0]
df = pd.DataFrame()
df['countries']=['GR'] #dm.countries()
df['values']=[3] # list(map(lambda x: len(x),dm.countries()))
location = 'DE'

@app.callback(
    Output('choro-graph', 'figure'),
    [Input('choro-graph', 'clickData')])
def update_figure(clickData):    
    if clickData is not None:            
        location = clickData['points'][0]['location']
        print(location)
    fig = px.scatter_mapbox(d, lat="lat", lon="lon", color='Production_Type',hover_name="name",hover_data=["capacity",'country'],zoom=3)
    fig.update_layout(mapbox_style="open-street-map",margin={"r":0,"t":0,"l":0,"b":0},
                      legend=dict(font=dict(size=10), orientation="h")
                      )
    # fig = px.choropleth_mapbox(df, geojson=geo, locations="countries", color='values',
    #                            #color_continuous_scale="Viridis",
    #                            featureidkey="properties.iso_a2",
    #                            #range_color=(0, 12),
    #                            mapbox_style="carto-positron", # open-street-map
    #                            zoom=3, center = {"lat": 51.0902, "lon": 10.7129},
    #                            #opacity=0.9,
    #                            labels={'values':'Werte'}
    #                           )
    return fig

############## Load Graph ##############################

@app.callback(
    Output("load_graph", "figure"),
    [
        Input("country_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
    ],
)
def make_load_figure(country_control, start_date, end_date, group_by_control):

    layout_count = copy.deepcopy(layout)
    start =datetime.strptime(start_date, '%Y-%m-%d').date()
    end =datetime.strptime(end_date, '%Y-%m-%d').date()
    g = dm.load(country_control,Filter(start,end,group_by_control))
    
    data = [
        dict(
            type="lines",
            mode="lines",
            x=g.index,
            y=g["value"],
            name="Load",
            opacity=0.8,
            
        ),
        # dict(
        #     type="bar",
        #     x=g.index,
        #     y=g["sum(0)"],
        #     name="Load bar",
        #     hoverinfo="skip",
        #     #marker=dict(color=colors),
        # ),
    ]

    layout_count["title"] = "Load for {} from {} to {}".format(country_control,start_date,end_date)
    #layout_count["dragmode"] = "select"
    layout_count["showlegend"] = True
    layout_count["autosize"] = True
    layout_count['hovermode']='x unified'

    figure = dict(data=data, layout=layout_count)
    return figure

############ Generation Graph   ##############

@app.callback(
    Output("generation_graph", "figure"),
    [
        Input("country_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
        Input("energy_type_selector", "value"),
    ],
)
def make_generation_figure(country_control, start_date, end_date, group_by_control,energy_type_selector):
    start =datetime.strptime(start_date, '%Y-%m-%d').date()
    end =datetime.strptime(end_date, '%Y-%m-%d').date()
    
    generation = dm.generation(country_control,Filter(start,end,group_by_control))
    del generation['country']    
    generation=generation/1000
    g = generation.melt(var_name='kind', value_name='value',ignore_index=False)
    
    if g.empty:
        return dict(data=[], layout=dict(title="No Data Found for current interval"))
    figure = px.area(g, x=g.index, y="value", color='kind',line_group="kind")
    figure.update_layout(title="Generation for {} from {} to {}".format(country_control,start_date,end_date),
                   xaxis_title=group_by_control,
                   yaxis_title='Generated Energy by Production kind in GW',
                   hovermode="closest",
                   legend=dict(font=dict(size=10), orientation="h"),)
    return figure

############ Capacity Graph   ##############

@app.callback(
    Output("capacity_graph", "figure"),
    [
        Input("country_control", "value")
    ],
)
def make_capacity_figure(country_control):

    # produces duplicates for FR even though distinct is used
    capacity = dm.capacity(country_control).drop_duplicates()
    del capacity['country']
    capacity=capacity/1000
    g = capacity.melt(var_name='kind', value_name='value',ignore_index=False)
    
    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}
    
    figure = px.bar(g, x=g.index, y="value", color='kind', text='value')#bar_group="kind")
    figure.update_layout(title="Generation capacity for {} per year".format(country_control),
                   xaxis_title='years',
                   yaxis_title='Capacity by Production kind',)
    figure.update_traces(texttemplate='%{text:.2s}', textposition='inside')
    figure.update_yaxes(ticksuffix="GW")
    return figure

app.run_server(debug=True, use_reloader=False)