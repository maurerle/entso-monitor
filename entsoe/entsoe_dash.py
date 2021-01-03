#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 23:31:45 2020

@author: maurer
"""

from urllib.parse import urlparse, parse_qsl, urlencode
from dash.exceptions import PreventUpdate
import dash
from datetime import datetime, date
import pandas as pd
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import plotly.express as px
import dash_table

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
    from entsoe_sqlite_manager import EntsoeSQLite, EntsoePlantSQLite
    # data manager
    dm = EntsoeSQLite('data/entsoe.db')
    # plant data manager
    pdm = EntsoePlantSQLite('data/entsoe-plant.db')
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
    # TODO update SPARK
    pdm = EntsoePlantSQLite('data/entsoe-plant.db')

powersys = dm.powersystems('')
powersys['capacityName'] = powersys['capacity'].apply(lambda x: str(x)+' MW')

climate = dm.climateImpact()
climateList = list(climate.columns)
climateList.append('Keine')
appname = 'Entsoe Monitor'

color_map = {
    "Biomass": "DarkGreen",
    "Fossil Brown coal/Lignite": "SaddleBrown",
    "Fossil Gas": "blue",
    "Fossil Hard coal": "DimGrey",
    "Fossil Oil": "black",
    "Geothermal": "FireBrick",
    "Hydro Pumped Storage": "DodgerBlue",
    "Hydro Run-of-river and poundage": "LightSeaGreen",
    "Other": "Violet",
    "Other renewable": "Thistle",
    "Solar": "yellow",
    "Waste": "Tan",
    "Wind Offshore": "Steelblue",
    "Wind Onshore": "DeepSkyBlue",
    "Fossil Coal-derived gas": "RosyBrown",
    "Nuclear": "purple",
    "Hydro Water Reservoir": "DarkKhaki",
    "Marine": "DarkBlue",
    "Fossil Oil shale": "DarkGoldenRod",
    "Fossil Peat": "Coral",
}


def cmap(index):
    return list(map(lambda x: color_map[x], index))


layout = html.Div(
    [dcc.Location(id='url', refresh=False),
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
            [
                html.Div(
                    [
                        html.P(
                            "Select Climate metric:",
                            className="control_label",
                        ),
                        dcc.Dropdown(id='climate_picker',
                                     options=[{'label': x, 'value': x}
                                              for x in climateList],
                                     value='Keine',
                                     clearable=False,),
                        html.P("Map Data:", className="control_label"),
                        dcc.Dropdown(id="map_control",
                                     options=[{'label': 'Countries', 'value': 'countries'},
                                              {'label': 'Plants',
                                                  'value': 'plants'},
                                              {'label': 'Both', 'value': 'plants,countries'}],
                                     value='countries',
                                     className="dcc_control",
                                     clearable=False,
                                     ),
                        html.P(
                            "Select Time Filter:",
                            className="control_label",
                        ),
                        #dcc.Dropdown(options=[{'label':x, 'value': x} for x in range(2015, 2020)]),
                        dcc.DatePickerRange(
                            id='date_picker',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date(2020, 12, 19),
                            start_date=date(2015, 8, 21),
                            end_date=date(2015, 9, 4),
                            display_format='DD.MM.YY',
                            # initial_visible_month='2020-08-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Country:", className="control_label"),
                        dcc.Dropdown(id="country_control",
                                     options=[{'label': x, 'value': x}
                                              for x in dm.countries()],
                                     value='FR',
                                     className="dcc_control",
                                     clearable=False,
                                     ),

                        html.P("Group by:", className="control_label"),
                        dcc.RadioItems(
                            id="group_by_control",
                            options=[
                                {"label": "Year", "value": "year"},
                                {"label": "Month", "value": "month"},
                                {"label": "Day ", "value": "day"},
                                {"label": "Hour", "value": "hour"},
                                {"label": "1/4 hour", "value": "minute"},
                            ],
                            value="day",
                            labelStyle={"display": "inline-block"},
                            className="dcc_control",
                        ),
                    ],
                    className="pretty_container three columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.P(
                            "Country map including location of conventional energy sources",
                            className="control_label",
                        ),
                        dcc.Graph(id='choro_graph',
                                  config={"displaylogo": False}),
                    ],
                    id="locationMapContainer",
                    className="pretty_container nine columns",
                ),
            ],
            className="row flex-display",
    ),
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(
            [dcc.Tabs([
                dcc.Tab(label='Energy Generation', children=[dcc.Graph(
                    id="generation_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Energy Load', children=[dcc.Graph(
                    id="load_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Crossborder Flows', children=[dcc.Graph(
                    id="neighbour_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Generation Capacity', children=[dcc.Graph(
                    id="capacity_graph", config={"displaylogo": False})]),
                dcc.Tab(label='Generation per Plant', children=[
                    dcc.Dropdown(id="plant_control",

                                 options=[{'label': x.encode('latin-1').decode('utf-8'), 'value': x}
                                          for x in pdm.getNames()['name']],
                                 value=[
                                     'DOEL 2'],
                                 multi=True,
                                 className="dcc_control",),
                    dcc.Graph(id="per_plant", config={"displaylogo": False})
                ]),
            ]),
                html.P("Units are average values over the selected time period"),
            ],
            id="graphTabContainer",
            className="pretty_container",
    ),
        html.Div(
            [html.P("Power plants:", className="control_label"),
             dash_table.DataTable(
                id='plantTable',
                columns=[{"name": i, "id": i} for i in powersys.columns],
                data=powersys.to_dict('records'),
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
    [Input('choro_graph', 'clickData'),
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


component_ids = ['start_date', 'end_date', 'group_by_control','country_control','climate_picker','plant_control']


def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    return state


@app.callback([
    Output("date_picker", "start_date"),
    Output("date_picker", "end_date"),
    Output("group_by_control", "value"),
    Output("climate_picker", "value"),
    Output("plant_control", "value"),
    #Output("country_control", "value"),
   ],
    inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = parse_state(href)
    print(href)
    # for element in elements
    if all(element in state for element in component_ids):
        plants = state['plant_control'].strip("][").replace("'",'').split(',')
        print(plants[1].encode('latin-1'))
        return state['start_date'], state['end_date'], state['group_by_control'], state['climate_picker'], plants #, state['country_control']
    else:
        raise PreventUpdate


@app.callback(Output('url', 'search'),
              [
    Input("date_picker", "start_date"),
    Input("date_picker", "end_date"),
    Input("group_by_control", "value"),
    Input("country_control", "value"),
    Input("climate_picker", "value"),
    Input("plant_control", "value"),
])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids, values)))
    return f'?{state}'

############ Plant Graph ############


@app.callback(
    Output("per_plant", "figure"),
    [
        Input("plant_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
    ],
)
def make_load_figure(plants, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    g = pdm.plantGen(plants, Filter(start, end, group))
    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}
    g['value'] /= 1e3
    plantNames = list(map(lambda x: x.encode(
        'latin-1').decode('utf-8'), plants))
    figure = px.line(g, x=g.index, y="value", color='name')
    figure.update_layout(title=f"Generation for {', '.join(plantNames)} from {start_date} to {end_date}",
                         #xaxis_title=group,
                         yaxis_title='Generation in GW for each interval',
                         autosize=True,
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="h"),)
    figure.update_yaxes(ticksuffix=" GW")
    return figure


############ Map   ##############


@app.callback(
    Output('choro_graph', 'figure'),
    [Input("climate_picker", "value"),
     Input("map_control", "value"), ])
def update_figure(climate_sel, mapSelection):

    countries = dm.countries()
    data = []
    if 'countries' in mapSelection:

        if not climate_sel in climate.columns:
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
                                        thickness=25, ticklen=3, title='Austoß in g/kWh'),
                                    geojson=geo,
                                    featureidkey="properties.iso_a2",
                                    text=countries,
                                    # below=True,
                                    hovertemplate='<b>Country</b>: <b>%{text}</b>' +
                                    '<br><b>Austoß pro kWh </b>: %{z} g<br>',
                                    marker_line_width=0.1, marker_opacity=0.8,
                                    )
        data.append(choro)

    if 'plants' in mapSelection:
        vals = powersys['Production_Type'].unique()
        for val in vals:
            d = powersys[powersys['Production_Type'] == val]
            scatt = go.Scattermapbox(lat=d['lat'], name=val,
                                     lon=d['lon'],
                                     mode='markers',
                                     text=d["name"],
                                     hovertext=d[[
                                         'name', 'capacityName', 'country']],
                                     hoverinfo=['text'],
                                     below='',
                                     marker=dict(size=6, color=color_map[val])
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

    capacity.fillna(0, inplace=True)
    capacity /= 1e3
    capacity = capacity.loc[:, (capacity != 0).any(axis=0)]
    g = capacity.melt(var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}

    figure = px.bar(g, x=g.index, y="value", color='kind', color_discrete_map=color_map,
                    text='value')  # bar_group="kind")
    figure.update_layout(title="Generation capacity for {} per year".format(country_control),
                         xaxis_title='',
                         yaxis_title='Capacity by Production kind',)
    figure.update_traces(texttemplate='%{text:.2s}', textposition='inside')
    figure.update_yaxes(ticksuffix=" GW")
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
    g /= 1e3
    figure = px.line(g, x=g.index, y="value")
    figure.update_layout(title="Load for {} from {} to {}".format(country_control, start_date, end_date),
                         xaxis_title='',
                         yaxis_title='Load in GW for each interval',
                         autosize=True,
                         hovermode="x unified",
                         legend=dict(font=dict(size=10), orientation="h"),)
    figure.update_yaxes(ticksuffix=" GW")
    return figure

############ Generation Graph   ##############


@app.callback(
    Output("generation_graph", "figure"),
    [
        Input("country_control", "value"),
        Input("date_picker", "start_date"),
        Input("date_picker", "end_date"),
        Input("group_by_control", "value"),
        Input("climate_picker", "value"),
    ],
)
def make_generation_figure(country_control, start_date, end_date, group, climate_sel):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    generation = dm.generation(country_control, Filter(start, end, group))
    del generation['country']

    desc = 'energy generation by production kind in GW'

    unit = 'GW'
    generation /= 1e3
    if climate_sel in climate.columns:
        generation = generation*climate[climate_sel]
        unit = 'tons'
        desc = climate_sel+' in '+unit

    generation.fillna(0, inplace=True)
    generation = generation.loc[:, (generation != 0).any(axis=0)]
    g = generation.melt(
        var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return dict(data=[], layout=dict(title="No Data Found for current interval"))
    figure = px.area(g, x=g.index, y="value", color='kind',
                     color_discrete_map=color_map, line_group="kind")
    figure.update_layout(title=desc+" for {} from {} to {}".format(country_control, start_date, end_date),
                         xaxis_title='',
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
    ],
)
def make_neighbour_figure(country_control, start_date, end_date, group_by_control):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    g = dm.crossborderFlows(
        country_control, Filter(start, end, group_by_control))

    if g.empty:
        return dict(data=[], layout=dict(title=f"No Data for {country_control} from {start_date} to {end_date}"))
    fig = go.Figure()
    for col in g.columns:
        fig.add_trace(go.Scatter(x=g.index, y=g[col]/1e3,
                                 mode='lines',
                                 name=col))

    fig.update_layout(title=f"Netto Export for {country_control} from {start_date} to {end_date}",
                      #xaxis_title=group,
                      yaxis_title='Exported to neighbour - imported in GWh',
                      hovermode="closest",
                      showlegend=True,
                      legend=dict(font=dict(size=10), orientation="h"),)
    fig.update_yaxes(ticksuffix=' GW')
    return fig


if __name__ == "__main__":
    app.layout = layout
    app.run_server(debug=True, use_reloader=False, host='0.0.0.0', port=8051)
