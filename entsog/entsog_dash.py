#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 00:57:00 2020

@author: maurer
"""

import dash
from datetime import datetime, date
import pandas as pd
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
import dash_table
from entsog_data_manager import Filter
if __name__ == "__main__":
    app = dash.Dash(__name__, meta_tags=[
                    {"name": "viewport", "content": "width=device-width"}])
    server = app.server
else:
    from app import app

if True:
    from entsog_sqlite_manager import EntsogSQLite
    dm = EntsogSQLite('data/entsog.db')
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
    dm = EntsogParquet('data', spark)

# initialize data
incons = dm.interconnections()
bzs = dm.balancingzones()
connectionPoints = dm.connectionpoints()
operators = dm.operators()

# a = oppointdir['tpTsoBalancingZone']== operators[operators['operatorKey'] in ]
oppointdir = dm.operatorpointdirections()
opd = oppointdir.copy()
del opd['directionKey']

opd = opd.drop_duplicates()

defaultBZ = 'Austria'
inter = incons[incons['fromBzLabel'] == defaultBZ]
points = inter['pointLabel'].dropna().unique()
pointKeys = inter['pointKey'].dropna().unique()
allPointOptions = [{'label': points[i], 'value': pointKeys[i]}
                   for i in range(len(points))]

available_maplayers = ['countries_zones', 'pipelines_small_medium_large', 'pipelines_medium_large',
                       'pipelines_large', 'drilling_platforms', 'gasfields', 'projects', 'country_names']
standard_layers = ['countries_zones', 'pipelines_small_medium_large']
appname = 'ENTSOG Monitor'

# initialize layout
layout = html.Div(
    [
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
                                "marginBottom": "25px",
                                "backgroundColor": 'white',
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
                                    style={"marginBottom": "0px"},
                                ),
                                html.H5(
                                    "Transmission Overview", style={"marginTop": "0px"}
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
            style={"marginBottom": "25px"},
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
                            start_date=date(2017, 7, 2),
                            end_date=date(2017, 7, 11),
                            display_format='DD.MM.YY',
                            # initial_visible_month='2015-02-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Balancing Zone:", className="control_label"),
                        dcc.Dropdown(id="bz_control",
                                     options=[{'label': x, 'value': x} for x in list(
                                         dm.balancingzones()['bzLabel'])],
                                     value=defaultBZ,
                                     className="dcc_control",
                                     ),
                        html.P("Operators:", className="control_label"),
                        dcc.Dropdown(id="operator_control",
                                     options=[{'label': x, 'value': y} for x, y in operators[[
                                         'operatorKey', 'operatorLabel']].values.tolist()],
                                     # value=[''],
                                     multi=True,
                                     className="dcc_control",
                                     ),
                        html.P("Points:", className="control_label"),
                        dcc.Dropdown(id="point_control",


                                     options=allPointOptions,
                                     # value=[''],
                                     multi=True,
                                     className="dcc_control",
                                     ),
                        html.P("Map Layer:", className="control_label"),
                        dcc.Dropdown(id="map_layer_control",
                                     options=[
                                         {'label': x.replace('_', ' '), 'value': x} for x in available_maplayers],
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
                            [dcc.Graph(id="point_map", animate=True,
                                       config={"displaylogo": False})],
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
            dcc.Tabs([
                dcc.Tab(label='Sum for Region', children=[dcc.Graph(
                    id="points_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Crossborder Zone', children=[dcc.Graph(
                    id="crossborder_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Sum per Infrastructure', children=[dcc.Graph(
                    id="infrastructure_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Selected Points', children=[dcc.Graph(
                    id="points_label_graph", config={"displaylogo": False})]),
            ]),
            id="graphTabContainer",
            className="pretty_container",
        ),
        html.Div(
            [html.P("Operator Point Directions:", className="control_label"),
             dash_table.DataTable(
                id='opdTable',
                columns=[{"name": i, "id": i} for i in opd.columns],
                data=opd.to_dict('records'),
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                page_action="native",
                page_current=0,
                page_size=25,
                style_table={'overflowX': 'auto'},
            )],
            id="tableContainer",
            className="pretty_container",
        ),
    ])

############ Controls   ##############


@app.callback(
    Output("point_control", "options"),
    [
        Input("operator_control", "value"),
        Input("bz_control", "value"),
    ],
)
def updatePointControl(operatorKey, bz):
    if operatorKey != None and len(operatorKey) > 0:
        inter = incons[incons['fromOperatorKey'].apply(
            lambda x: x in operatorKey)]
    elif bz != None and len(bz) > 0:
        inter = incons[incons['fromBzLabel'] == bz]
    else:
        inter = incons
    points = inter['pointLabel'].dropna().unique()
    pointKeys = inter['pointKey'].dropna().unique()
    return [{'label': points[i], 'value': pointKeys[i]} for i in range(len(points))]


@app.callback(
    Output("point_control", "value"),
    [
        Input('point_map', 'clickData'),
        Input('point_map', 'selectedData')
    ],
)
def updateSelectedPoints(clickData, selectedData):
    if selectedData is not None and len(selectedData) > 0:
        points = selectedData
    elif clickData is not None and len(clickData) > 0:
        points = clickData
    else:
        return []

    pointKeys = []
    for point in points['points']:
        if 'customdata' in point:
            pointKeys.append(point['customdata'][1])

    return list(set(pointKeys))


@app.callback(
    Output("operator_control", "options"),
    [
        Input("bz_control", "value"),
    ],
)
def updateOperatorControl(bz):
    if bz != None:
        inter = incons[incons['fromBzLabel'] == bz]
    else:
        inter = incons
    opt = inter['fromOperatorLabel'].dropna().unique()
    optKeys = inter['fromOperatorKey'].dropna().unique()
    return [{'label': opt[i], 'value': optKeys[i]} for i in range(len(opt))]

############ Map       ##############


@app.callback(
    Output("point_map", "figure"),
    [
        Input("map_layer_control", "value"),
        Input("point_control", "options")
    ],
)
def makePointMap(layer_control, options):
    layers = []

    df = pd.DataFrame(options)
    if df.empty:
        inter = incons
    else:
        points = list(df['value'])
        inter = incons[incons['pointKey'].apply(lambda x: x in points)]

    for layer in layer_control:
        layers.append({"below": 'traces',
                       "sourcetype": "raster",
                       "sourceattribution": '<a href="https://transparency.entsog.eu/#/map">ENTSO-G Data</a>',
                       "source": [
                           "https://datensch.eu/cdn/entsog/"+layer+"/{z}/{x}/{y}.png"]})
    #inter=inter[['lat','lon',"pointLabel","pointKey", "fromOperatorLabel",'fromCountryKey','toOperatorLabel',"toCountryKey"]].drop_duplicates()
    fig = px.scatter_mapbox(inter, lat="lat", lon="lon", color='fromCountryKey',
                            custom_data=["pointLabel", "pointKey", "fromOperatorLabel",
                                         'fromCountryKey', 'toOperatorLabel', "toCountryKey"],
                            zoom=1, height=600)

    fig.update_traces(
        hovertemplate='</br><b>%{customdata[0]}</b> - %{customdata[1]}</br> from: %{customdata[2]}, %{customdata[3]} </br> to:     %{customdata[4]}, %{customdata[5]}')

    fig.update_layout(mapbox_style="white-bg", mapbox_layers=layers,
                      margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig

############ Graphs   ##############


@app.callback(
    Output("points_graph", "figure"),
    [
        Input("operator_control", "value"),
        Input("bz_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value")
    ],
)
def updateFlowGraph(operator, bz, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    p = pd.DataFrame()
    a = pd.DataFrame()

    desc = 'no valid points'
    if operator != None and len(operator) > 0:
        inter = incons[incons['fromOperatorKey'].apply(
            lambda x: x in operator)]
        desc = ', '.join(inter['fromOperatorLabel'].unique())
    elif bz != None:
        inter = incons[incons['fromBzLabel'] == bz]
        operator = list(inter['fromOperatorKey'].dropna().unique())
        desc = bz
    else:
        # TODO show usage for single pipeline
        operator = ['Nord Stream']
        desc = operator[0]

    if operator != None and len(operator) > 0:
        p = dm.operationaldata(operator, Filter(start, end, group))
        a = dm.operationaldata(operator, Filter(
            start, end, group), table='Allocation')

    if p.empty and a.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {desc} from {start_date} to {end_date}")}

    a = a.pivot(columns=['directionKey'], values='value')
    p = p.pivot(columns=['directionKey'], values='value')

    if 'entry' in a.columns and 'exit' in a.columns:
        a['usage'] = a['entry']-a['exit']

    if 'entry' in p.columns and 'exit' in p.columns:
        p['usage'] = p['entry']-p['exit']

    a.columns = list(map(lambda x: 'alloc_'+x, a.columns))
    p.columns = list(map(lambda x: 'physical_'+x, p.columns))

    a = a/1e6
    p = p/1e6
    figure = go.Figure()
    for column in p.columns:
        figure.add_trace(go.Scatter(
            x=p.index, y=p[column], mode='lines', name=column))

    for column in a.columns:
        figure.add_trace(go.Scatter(
            x=a.index, y=a[column], mode='lines', name=column))

    figure.update_layout(title=f"Flow in GWh/{group} for {desc} from {start_date} to {end_date}",
                         xaxis_title=group,
                         yaxis_title=f'Transferred Energy in GWh/{group}',
                         hovermode="closest",
                         legend=dict(font=dict(size=10), orientation="v"),)
    figure.update_yaxes(ticksuffix=" GWh")
    return figure


@app.callback(
    Output("points_label_graph", "figure"),
    [
        Input("point_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
        Input("point_control", "options")
    ],
)
def updatePointsLabelGraph(points, start_date, end_date, group, options):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    desc = 'no valid points'

    if points != None and len(points) > 0:
        # include with toPointKey:
        inter = incons[incons['pointKey'].apply(lambda x: x in points)]

        # select both from and to points here
        points = list(set(points) | set(inter['toPointKey'].unique()))

        valid_points = [x for x in points if x is not None]

        if len(valid_points) < 5:
            desc = ', '.join(valid_points)
        else:
            desc = str(len(valid_points)) + ' points'
    else:
        valid_points = []
        #valid_points = list(map(lambda x: x['value'],options))

    g = pd.DataFrame()
    if len(valid_points) > 0:
        p = dm.operationaldataByPoints(valid_points, Filter(
            start, end, group), ['pointKey', 'directionKey'])
        a = dm.operationaldataByPoints(valid_points, Filter(start, end, group), [
                                       'pointKey', 'directionKey'], table='Allocation')
        p['indicator'] = 'phys'
        a['indicator'] = 'alloc'

        g = pd.concat([a, p])
    if g.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {desc} from {start_date} to {end_date}")}

    g['point'] = g['directionKey']+' '+g['pointLabel']+' '+g['indicator']
    g['value'] = g['value']/1e6  # show in GW

    # sort values alphabetically for better visualization
    ordered = g['point'].unique()
    ordered.sort()

    figure = px.line(g, x=g.index, y="value", color='point', line_group="point", custom_data=[
                     'operatorLabel', 'pointKey'], category_orders={'point': list(ordered)})
    figure.update_traces(
        hovertemplate='<b>%{y}</b>, %{customdata[0]}, %{customdata[1]}')
    figure.update_layout(title=f"Flow in GWh/{group} {desc} from {start_date} to {end_date}",
                         xaxis_title=group,
                         yaxis_title=f'Transferred Energy in GWh/{group}',
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="v"),)
    figure.update_yaxes(ticksuffix=" GWh")
    return figure


@app.callback(
    Output("crossborder_graph", "figure"),
    [
        Input("bz_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
    ],
)
def updateCrossborderGraph(bz, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    c = pd.DataFrame()

    if bz == None or len(bz) < 1:
        return {'data': [], 'layout': dict(title='No zone selected')}

    c = dm.crossborder(bz, Filter(start, end, group))

    if c.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {bz} from {start_date} to {end_date}")}

    c = c/1e6

    figure = go.Figure()
    for column in c.columns:
        figure.add_trace(go.Scatter(
            x=c.index, y=c[column], mode='lines', name=column))

    figure.update_layout(title=f"Crossborder Flow in GWh/{group} for {bz} from {start_date} to {end_date}",
                         xaxis_title=group,
                         yaxis_title=f'Imported Energy in GWh/{group}',
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="v"),)
    figure.update_yaxes(ticksuffix=" GWh")

    return figure

# Infrastructure Graph ###########3


@app.callback(
    Output("infrastructure_graph", "figure"),
    [
        Input("bz_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
    ],
)
def updateInfrastructureGraph(bz, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    c = pd.DataFrame()

    if bz == None or len(bz) < 1:
        return {'data': [], 'layout': dict(title='No zone selected')}

    c = dm.bilanz(bz, Filter(start, end, group))

    if c.empty:
        return {'data': [], 'layout': dict(title=f"No Data Found for {bz} from {start_date} to {end_date}")}

    c = c/1e6

    figure = go.Figure()
    for column in c.columns:
        figure.add_trace(go.Scatter(
            x=c.index, y=c[column], mode='lines', name=column))

    figure.update_layout(title=f"Flow per Infrastructure type in GWh/{group} for {bz} from {start_date} to {end_date}",
                         # xaxis_title=group,
                         yaxis_title=f'Energy added to {bz} in GWh/{group}',
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="v"),)
    figure.update_yaxes(ticksuffix=" GWh")

    return figure


if __name__ == "__main__":
    app.layout = layout
    app.run_server(debug=True, use_reloader=True, host='0.0.0.0', port=8050)
