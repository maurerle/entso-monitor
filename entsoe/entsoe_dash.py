#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 23:31:45 2020

@author: maurer
"""

import dash
from datetime import datetime, date
import pandas as pd
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import plotly.express as px

from entsoe_data_manager import Filter
import json
import copy

if __name__ == "__main__":
    app = dash.Dash(__name__, meta_tags=[
                    {"name": "viewport", "content": "width=device-width"}])
    server = app.server
else:
    from app import app

if True:
    from entsoe_sqlite_manager import EntsoeSQLite
    dm = EntsoeSQLite('data/entsoe.db')
else:
    from entsoe_parquet_manager import EntsoeParquet
    import findspark
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    try:
        findspark.init()

        spark
        print('using existing spark object')
    except:
        print('creating new spark object')
        conf = SparkConf().setAppName('entsoe').setMaster('local')
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    dm = EntsoeParquet('data', spark)

powersys = dm.powersystems('')
climate = dm.climateImpact()
climate.columns
appname = 'Entsoe Monitor'

layout = html.Div(
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
                                "backgroundColor": 'white',
                            },
                        )
                    ],
                    className="one-third column pretty_container",
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
            [dcc.Graph(id='choro-graph', config={"displaylogo": False})],
            id="locationMapContainer",
            className="pretty_container",
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P(
                            "Select Climate metric:",
                            className="control_label",
                        ),
                        dcc.Dropdown(id='climate_picker',
                                     options=[{'label': x, 'value': x}
                                              for x in list(climate.columns)],
                                     value='CO2 mit VK'),
                        html.P(
                            "Select Time Filter:",
                            className="control_label",
                        ),
                        #dcc.Dropdown(options=[{'label':x, 'value': x} for x in range(2015, 2020)]),
                        dcc.DatePickerRange(
                            id='date_picker',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date(2020, 10, 19),
                            start_date=date(2020, 8, 21),
                            end_date=date(2020, 9, 4),
                            display_format='DD.MM.YY',
                            initial_visible_month='2020-08-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Country:", className="control_label"),
                        dcc.Dropdown(id="country_control",
                                     options=[{'label': x, 'value': x}
                                              for x in dm.countries()],
                                     value='FR',
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
                        html.P("Energy generation type:",
                               className="control_label"),
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
            dcc.Tabs([
                dcc.Tab(label='Energy Generation', children=[dcc.Graph(
                    id="generation_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Energy Load', children=[dcc.Graph(
                    id="load_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Crossborder Flows', children=[dcc.Graph(
                    id="neighbour_graph", config={"displaylogo": False})]),
            ]),
            id="graphTabContainer",
            className="pretty_container",
        ),
    ])

app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("load_graph", "figure")],
)

with open("europe.geo.json", "r", encoding="utf-8") as f:
    geo = json.load(f)

df = pd.DataFrame()
df['countries'] = dm.countries()
df['values'] = list(map(lambda x: len(x), dm.countries()))


############ Controls   ##############

@app.callback(
    Output('country_control', 'value'),
    [Input('choro-graph', 'clickData'),
     State('country_control', 'value')])
def update_dropdown(clickData, prev):
    # zur initialisierung
    location = prev
    if clickData is not None:
        print(clickData['points'][0])
        if 'location' in clickData['points'][0]:
            location = clickData['points'][0]['location']
        elif 'text' in clickData['points'][0]:
            print(clickData['points'][0]['text'])
            # TODO click on energy plant

    return location

############ Map   ##############


@app.callback(
    Output('choro-graph', 'figure'),
    [Input("climate_picker", "value")])
def update_figure(climate_sel):

    countries = dm.countries()

    if climate_sel == None:
        values = list(map(lambda x: len(x), dm.countries()))
    else:
        values = []
        for country in countries:
            capacity = dm.capacity(country)
            del capacity['country']
            da = capacity*climate[climate_sel]

            if da.empty:
                values.append(0)
            else:
                gramSum = da.iloc[-1].sum()
                values.append(gramSum/capacity.iloc[-1].sum())

    choro = go.Choroplethmapbox(z=values,
                                locations=countries,
                                colorscale='algae',  # carto
                                colorbar=dict(
                                    thickness=20, ticklen=3, title='Austoß in g/kWh'),
                                geojson=geo,
                                featureidkey="properties.iso_a2",
                                text=countries,
                                # below=True,
                                hovertemplate='<b>Country</b>: <b>%{text}</b>' +
                                '<br><b>Austoß pro kWh </b>: %{z} g<br>',
                                marker_line_width=0.1, marker_opacity=0.8,
                                )

    vals = powersys['Production_Type'].unique()
    data = []
    data.append(choro)
    for val in vals:
        d = powersys[powersys['Production_Type'] == val]
        scatt = go.Scattermapbox(lat=d['lat'], name=val,
                                 lon=d['lon'],
                                 mode='markers+text',
                                 text=d["name"],
                                 hovertext=d[['name', 'capacity', 'country']],
                                 hoverinfo=['text'],
                                 below='',
                                 #marker=dict( size=12, color ='rgb(235, 0, 100)')
                                 )
        data.append(scatt)
    layout = go.Layout(title_text='Europe mapbox choropleth', title_x=0.5,  # width=750, height=700,
                       showlegend=True,
                       mapbox=dict(center={"lat": 50, "lon": 10},
                                   zoom=3,
                                   style="carto-positron"
                                   ),
                       margin={"r": 0, "t": 0, "l": 0, "b": 0},
                       legend=dict(font=dict(size=9), orientation="h"),
                       )
    return go.Figure(data=data, layout=layout)

############ Capacity Graph   ##############


@app.callback(
    Output("capacity_graph", "figure"),
    [
        Input("country_control", "value")
    ],
)
def make_capacity_figure(country_control):
    capacity = dm.capacity(country_control)
    del capacity['country']
    capacity /= 1000
    g = capacity.melt(var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}

    figure = px.bar(g, x=g.index, y="value", color='kind',
                    text='value')  # bar_group="kind")
    figure.update_layout(title="Generation capacity for {} per year".format(country_control),
                         xaxis_title='years',
                         yaxis_title='Capacity by Production kind',)
    figure.update_traces(texttemplate='%{text:.2s}', textposition='inside')
    figure.update_yaxes(ticksuffix="GW")
    return figure

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
def make_load_figure(country_control, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    g = dm.load(country_control, Filter(start, end, group))
    g /= 1000
    figure = px.line(g, x=g.index, y="value")
    figure.update_layout(title="Load for {} from {} to {}".format(country_control, start_date, end_date),
                         xaxis_title=group,
                         yaxis_title='Load in GW for each interval',
                         autosize=True,
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="h"),)
    figure.update_yaxes(ticksuffix="GW")
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
        Input("climate_picker", "value"),
    ],
)
def make_generation_figure(country_control, start_date, end_date, group, e_type, climate_sel):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    generation = dm.generation(country_control, Filter(start, end, group))
    del generation['country']

    desc = 'energy generation by production kind in GW'

    unit = 'GW'
    generation /= 1000
    if climate_sel != None:
        generation = generation*climate[climate_sel]
        unit = 'tons'
        desc = climate_sel+' in '+unit

    g = generation.melt(
        var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return dict(data=[], layout=dict(title="No Data Found for current interval"))
    figure = px.area(g, x=g.index, y="value", color='kind', line_group="kind")
    figure.update_layout(title=desc+" for {} from {} to {}".format(country_control, start_date, end_date),
                         xaxis_title=group,
                         yaxis_title=desc,
                         hovermode="closest",
                         legend=dict(font=dict(size=10), orientation="h"),)
    figure.update_yaxes(ticksuffix=' '+unit)
    return figure

############ Neighbour Graph   ##############


@app.callback(
    Output("neighbour_graph", "figure"),
    [
        Input("country_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
        Input("energy_type_selector", "value"),
    ],
)
def make_neighbour_figure(country_control, start_date, end_date, group_by_control, energy_type_selector):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    g = dm.crossborderFlows(
        country_control, Filter(start, end, group_by_control))

    if g.empty:
        return dict(data=[], layout=dict(title=f"No Data for {country_control} from {start_date} to {end_date}"))
    fig = go.Figure()
    for col in g.columns:
        fig.add_trace(go.Scatter(x=g.index, y=g[col],
                                 mode='lines',
                                 name=col))

    fig.update_layout(title=f"Netto Export for {country_control} from {start_date} to {end_date}",
                      xaxis_title=group_by_control,
                      yaxis_title='Exported to neighbour - imported in kWh',
                      hovermode="closest",
                      showlegend=True,
                      legend=dict(font=dict(size=10), orientation="h"),)
    return fig


if __name__ == "__main__":
    app.layout = layout
    app.run_server(debug=True, host='0.0.0.0', port=8051)
