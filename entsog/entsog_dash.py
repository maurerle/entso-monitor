#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 00:57:00 2020

@author: maurer
"""

import dash
from datetime import datetime, date
import pandas as pd
from dash.dependencies import Input, Output, ClientsideFunction,State
import dash_core_components as dcc
import dash_html_components as html
from entsog_data_manager import Filter
import plotly.express as px 

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
appname = "ENTSO-G Energy Monitor"
app.title = appname
server = app.server


if True:
    from entsog_sqlite_manager import EntsogSQLite
    dm= EntsogSQLite('entsog.db')
else:
    #from entsoe_parquet_manager import EntsogParquet
    import findspark
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    try:
        findspark.init()
        
        spark
        print('using existing spark object')
    except:
        print('creating new spark object')
        conf = SparkConf().setAppName('entsog').setMaster('local')
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    dm= EntsogParquet('data',spark)

interconnections=dm.interconnections()
bzs=dm.balancingzones()
connectionPoints=dm.connectionpoints()
operators=dm.operators()

#a = oppointdir['tpTsoBalancingZone']== operators[operators['operatorKey'] in ]
oppointdir=dm.operatorpointdirections()

available_maplayers = ['countries_zones','pipelines_small_medium_large','pipelines_medium_large','pipelines_large','drilling_platforms','gasfields','projects','country_names']
standard_layers = ['countries_zones','pipelines_small_medium_large']

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
                                    appname,
                                    style={"margin-bottom": "0px"},
                                ),
                                html.H5(
                                    "Transmission Overview", style={"margin-top": "0px"}
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
                            min_date_allowed=date(2017, 7, 1),
                            max_date_allowed=date(2020, 10, 19),
                            start_date=date(2017,7,2),
                            end_date=date(2017,7,11),
                            display_format='DD.MM.YY',
                            #initial_visible_month='2015-02-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Balancing Zone:", className="control_label"),
                        dcc.Dropdown(id="bz_control",
                                     options=[{'label':x, 'value': x} for x in list(dm.balancingzones()['bzLabel'])],
                                     value='NCG',
                                     className="dcc_control",
                                    ),
                        html.P("Operators:", className="control_label"),
                        dcc.Dropdown(id="operator_control",
                                     options=[{'label':x, 'value': y} for x,y in operators[['operatorKey','operatorLabel']].values.tolist()],
                                     #value=[''],
                                     multi=True,
                                     className="dcc_control",
                                    ),
                        html.P("Points:", className="control_label"),
                        dcc.Dropdown(id="point_control",
                                     options=[{'label':x, 'value': x} for x in dm.interconnections()['pointLabel'].unique()],
                                     #value=[''],
                                     multi=True,
                                     className="dcc_control",
                                    ),
                        html.P("Map Layer:", className="control_label"),
                        dcc.Dropdown(id="map_layer_control",
                                     options=[{'label':x.replace('_',' '), 'value': x} for x in available_maplayers],
                                     value=standard_layers,
                                     multi=True,
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
                    ],
                    className="pretty_container four columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="point_map", config={"displaylogo": False})],
                            id="mapContainer",
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
                            [dcc.Graph(id="points_graph")],
                            id="pointsGraphContainer",
                            className="pretty_container",
        ),
        html.Div(
                    [dcc.Graph(id="points_graph_op")],
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
    [Input("point_map", "figure")],
)


# dash_table.DataTable(
#     id='table',
#     columns=[{"name": i, "id": i} for i in df.columns],
#     data=df.to_dict('records'),
# )

############ Capacity Graph   ##############
@app.callback(
    Output("points_graph", "figure"),
    [
        Input("operator_control", "value"),
        Input("bz_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),        
        State("operator_control", "options")
    ],
)
def updateFlowGraph(operator, bz, start_date, end_date, group, options):
    start =datetime.strptime(start_date, '%Y-%m-%d').date()
    end =datetime.strptime(end_date, '%Y-%m-%d').date()
    g = pd.DataFrame()

    desc = 'invalid'
    if operator != None and len(operator)>0:
        inter = interconnections[interconnections['fromOperatorKey'].apply(lambda x: x in operator)]
        g = dm.physicalFlow(operator,Filter(start,end,group))
        desc = str(inter['fromOperatorLabel'])
    elif bz != None:
        inter = interconnections[interconnections['fromBzLabel']==bz]
        operator = inter['fromOperatorKey'].dropna().unique()
        desc = bz
        print(operator)
        g = dm.physicalFlow(operator,Filter(start,end,group))
    
    if g.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {desc} from {start_date} to {end_date}")}
    
    
    figure = px.line(g, x=g.index, y="value", color='directionKey',line_group="directionKey")
    figure.update_layout(title=f"Physical Flow in kWh/h for {desc} from {start_date} to {end_date}",
                   xaxis_title=group,
                   yaxis_title='Physical Flow in kWh/h',
                   hovermode="closest",
                   legend=dict(font=dict(size=10), orientation="h"),)
    return figure


@app.callback(
    Output("point_control", "options"),
    [
        Input("operator_control", "value"),
        Input("bz_control", "value"),
    ],
)
def updatePointControl(operatorKey, bz):
    print(operatorKey)
    if operatorKey != None:
        inter = interconnections[interconnections['fromOperatorKey'].apply(lambda x: x in operatorKey)]
    elif bz != None:
        inter = interconnections[interconnections['fromBzLabel'] ==bz]
    else:
        inter= interconnections
    points = inter['pointLabel'].dropna().unique()
    pointKeys = inter['pointKey'].dropna().unique()
    return [{'label':points[i], 'value': pointKeys[i]} for i in range(len(points))]


@app.callback(
    Output("operator_control", "options"),
    [
        Input("bz_control", "value"),
    ],
)
def updateOperatorControl(bz):
    if bz != None:
        inter = interconnections[interconnections['fromBzLabel']==bz]
    else:
        inter= interconnections
    opt = inter['fromOperatorLabel'].dropna().unique()
    optKeys = inter['fromOperatorKey'].dropna().unique()
    return [{'label':opt[i], 'value': optKeys[i]} for i in range(len(opt))]

@app.callback(
    Output("point_map", "figure"),
    [
        Input("bz_control", "value"),
        Input("operator_control", "value"),
        Input("map_layer_control", "value"),
        State('point_map', 'figure'),
    ],
)
def makePointMap(bz, ops, layer_control, curfig):
    layers = []
    inter =interconnections
    if bz != None:
        inter = interconnections[interconnections['fromBzLabel']==bz]
    if ops != None:
        inter = interconnections[interconnections['fromOperatorKey'].apply(lambda x: x in ops)]

    if inter.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {desc} from {start_date} to {end_date}")}
        #inter =interconnections
        # TODO handle inter.empty properly
        
    for layer in layer_control:
        layers.append({   "below": 'traces',
                    "sourcetype": "raster",
                    "sourceattribution": '<a href="https://transparency.entsog.eu/#/map">ENTSO-G Data</a>',
                    "source": [
                    "https://datensch.eu/cdn/entsog/"+layer+"/{z}/{x}/{y}.png"]})
    fig = px.scatter_mapbox(inter, lat="lat", lon="lon", hover_name="pointLabel", color='fromCountryKey',
                            hover_data=["pointKey", "fromCountryKey",'fromOperatorLabel','toOperatorLabel',"toCountryKey"],
                            zoom=1, height=600)
    
    fig.update_layout(mapbox_style="white-bg",mapbox_layers=layers,margin={"r":0,"t":0,"l":0,"b":0})
    return fig

if __name__ == "__main__":  
    app.run_server(debug=True, use_reloader=False, host='0.0.0.0', port=8052)
