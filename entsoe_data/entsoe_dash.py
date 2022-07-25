#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 23:31:45 2020

@author: maurer
"""

from datetime import datetime, date, timedelta
from urllib.parse import urlparse, parse_qsl, urlencode
import dash
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc
from dash import html
from dash import dash_table
import plotly.graph_objects as go
import plotly.express as px

from entsoe_data_manager import Filter
import json
import pandas as pd
import os

DATABASE_URI = 'postgresql://readonly:readonly@localhost:5432/entsoe'
DATABASE_URI = os.getenv('DATABASE_URI_ENTSOE','data/entsoe.db')
ENTSOE_URL = 'https://transparency.entsoe.eu/content/static_content/Static content/web api/Guide.html#_areas'

if __name__ == "__main__":
    app = dash.Dash(__name__, meta_tags=[
                    {"name": "viewport", "content": "width=device-width"}])
    server = app.server
else:
    from app import app

from entsoe_sqlite_manager import EntsoeSQLite, EntsoePlantSQLite
# data manager
dm = EntsoeSQLite(DATABASE_URI)
# plant data manager
pdm = EntsoePlantSQLite(DATABASE_URI)

powersys = pdm.powersystems('')
powersys['capacityName'] = powersys['capacity'].apply(lambda x: str(x)+' MW')

climate = dm.climateImpact()
climate.index = [item.lower() for item in climate.index]
climateList = list(climate.columns)
climateList.append('None')
appname = 'Entsoe Monitor'

color_map = {
    "Biomass": "DarkGreen",
    "Fossil Brown coal/Lignite": "SaddleBrown",
    "Fossil Coal-derived gas": "RosyBrown",
    "Fossil Gas": "blue",
    "Fossil Hard coal": "DimGrey",
    "Fossil Oil": "black",
    "Fossil Oil shale": "DarkGoldenRod",
    "Fossil Peat": "Coral",
    "Geothermal": "FireBrick",
    "Hydro Pumped Storage": "DodgerBlue",
    "Hydro Run-of-river and poundage": "LightSeaGreen",
    "Hydro Water Reservoir": "DarkKhaki",
    "Nuclear": "purple",
    "Other": "Violet",
    "Other renewable": "Thistle",
    "Solar": "yellow",
    "Waste": "Tan",
    "Wind Offshore": "Steelblue",
    "Wind Onshore": "DeepSkyBlue",
    "Marine": "DarkBlue",
}
color_map = dict((k.lower(), v) for k, v in color_map.items())

def cmap(index):
    return list(map(lambda x: color_map[x.lower()], index))


layout = html.Div(
    [dcc.Location(id='url_entsoe', refresh=False),
        html.Div(
            [
                html.Div(
                    [
                        html.H1(
                            appname,
                            style={"margin-bottom": "0px"},
                        ),
                        html.H5(
                            "Production Overview", style={"margin-top": "70px"},
                        ),
                    ],
                    className="eleven columns",
                    id="title",
                ),
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("fh-aachen.png"),
                            id="plotly-image",
                            style={
                                "height": "200px",
                                "width": "60px",
                                "margin-bottom": "0px",
                                "backgroundColor": 'white',
                            },
                        )
                    ],
                    className="one columns",
                ),
            ],
            id="header",
            className="row flex-display pretty_container",
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
                                     value='None',
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
                        # dcc.Dropdown(options=[{'label':x, 'value': x} for x in range(2015, 2020)]),
                        dcc.DatePickerRange(
                            id='datepicker',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date.today()+timedelta(days=30),
                            start_date=date.today()-timedelta(days=30),
                            end_date=date.today(),
                            display_format='DD.MM.YY',
                            # initial_visible_month='2020-08-01',
                            show_outside_days=True,
                            start_date_placeholder_text='MMMM Y, DD'
                        ),
                        html.P("Country:", className="control_label"),
                        dcc.Dropdown(id="country_control",
                                     options=[{'label': x, 'value': x}
                                              for x in dm.countries()['name']],
                                     value='FR',
                                     className="dcc_control",
                                     clearable=False,
                                     ),
                        dcc.Link(
                            'Meaning of Zone names', href=ENTSOE_URL),
                        html.P("Aggregation Intervall:",
                               className="control_label"),
                        dcc.RadioItems(
                            id="groupby_control",
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
                dcc.Tab(label='Generation per Plant', children=[
                    dcc.Dropdown(id="plant_control",

                                 options=[{'label': x, 'value': x}
                                          for x in pdm.getNames()['name']],
                                 value=[
                                     'DOEL 2'],
                                 multi=True,
                                 className="dcc_control",),
                    dcc.Graph(id="per_plant", config={"displaylogo": False})
                ]),
                dcc.Tab(label='Generation Capacity', children=[dcc.Graph(
                    id="capacity_graph", config={"displaylogo": False})]),
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
        html.Div([
            dcc.Link('Data comes from ENTSO-E Transparency Platform',
                     href='https://transparency.entsoe.eu/', refresh=True),
            html.Br(),
            dcc.Link('Legal Notice', refresh=True,
                     href='https://demo.nowum.fh-aachen.de/info.html'),
        ],
        className="pretty_container",
    ),
    ])

with open("europe.geo.json", "r", encoding="utf-8") as f:
    geo = json.load(f)

############ Controls   ##############


@app.callback(
    Output('country_control', 'value'),
    Output('plant_control', 'value'),
    [Input('choro_graph', 'clickData'),
     Input('url_entsoe', 'href')])
def update_dropdown(clickData, href):
    # zur initialisierung
    if clickData is None:
        if href:
            state = parse_state(href)
            if all(element in state for element in ['country_control', 'plant_control']):
                plants = state['plant_control'].strip(
                    "][").replace("'", '').split(',')
                return state['country_control'], plants
    else:
        print(clickData['points'][0])
        if 'location' in clickData['points'][0]:
            country = clickData['points'][0]['location']
            return country, dash.no_update

        elif 'text' in clickData['points'][0]:
            plant = clickData['points'][0]['text']
            return dash.no_update, [plant]

    return 'FR', ['DOEL 2']


component_ids = ['start_date', 'end_date', 'groupby_control',
                 'country_control', 'climate_picker', 'plant_control']


def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    return state


@app.callback([
    Output("datepicker", "start_date"),
    Output("datepicker", "end_date"),
    Output("groupby_control", "value"),
    Output("climate_picker", "value"),
],
    inputs=[Input('url_entsoe', 'href')])
def page_load(href):
    if not href:
        return []
    state = parse_state(href)
    print(href)
    # for element in elements
    if all(element in state for element in ['start_date', 'end_date', 'groupby_control', 'climate_picker']):
        return state['start_date'], state['end_date'], state['groupby_control'], state['climate_picker']
    else:
        raise PreventUpdate


@app.callback(Output('url_entsoe', 'search'),
              [
    Input("datepicker", "start_date"),
    Input("datepicker", "end_date"),
    Input("groupby_control", "value"),
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
        Input("datepicker", "start_date"),
        Input("datepicker", "end_date"),
        Input("groupby_control", "value"),
    ],
)
def make_load_figure(plants, start_date, end_date, group):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    g = pdm.plantGen(plants, Filter(start, end, group))
    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}
    g['value'] /= 1e3
    plantNames = list(plants)
    figure = px.line(g, x=g.index, y="value", color='name')
    figure.update_layout(title=f"Generation for {', '.join(plantNames)} from {start_date} to {end_date}",
                         # xaxis_title=group,
                         yaxis_title='avg Generation in GW for each interval',
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

    countries = list(dm.countries()['name'])
    data = []
    if 'countries' in mapSelection:
        showScale = True
        legend_title = 'Exhaust in g/kWh'
        hover_title = 'Exhaust per kWh'
        unit = 'g'

        if not climate_sel in climate.columns:
            values = list(map(lambda x: 1, countries))
            showScale = False
        else:
            # last element is ownconsumption
            if climate_sel == climate.columns[-1]:
                legend_title = 'Consumption in Wh/kWh'
                hover_title = 'Consumption per gen. kWh'
                unit = 'Wh'

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
                                        thickness=25, ticklen=3, title=legend_title),
                                    geojson=geo,
                                    featureidkey="properties.iso_a2",
                                    text=countries,
                                    # below=True,
                                    showscale=showScale,
                                    hovertemplate='<b>Country</b>: <b>%{text}</b>' +
                                    '<br><b>'+hover_title+'</b>: %{z} '+unit,
                                    marker_line_width=0.1, marker_opacity=0.8,
                                    )
        data.append(choro)

    if 'plants' in mapSelection:
        vals = powersys['production_type'].unique()
        for val in vals:
            d = powersys[powersys['production_type'] == val]
            scatt = go.Scattermapbox(lat=d['lat'], name=val,
                                     lon=d['lon'],
                                     mode='markers',
                                     text=d["entsoe_name"],
                                     hovertext=d[[
                                         'entsoe_name', 'capacityName', 'country']],
                                     hoverinfo=['text'],
                                     showlegend=True,
                                     below='',
                                     marker=dict(size=6, color=color_map[val.lower()])
                                     )
            data.append(scatt)
    layout = go.Layout(title_text='Europe mapbox choropleth', title_x=0.5,  # width=750, height=700,
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

    capacity = capacity.fillna(value=0)
    capacity /= 1e3
    capacity = capacity.loc[:, (capacity != 0).any(axis=0)]
    g = capacity.melt(var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return {'data': [], 'layout': dict(title="No Data Found for current interval")}

    figure = px.bar(g, x=g.index, y="value", color='kind', 
                    color_discrete_map=color_map,
                    category_orders={'kind': color_map.keys()},
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
        Input("datepicker", "start_date"),
        Input("datepicker", "end_date"),
        Input("groupby_control", "value"),
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
                         yaxis_title='avg Load in GW for each interval',
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
        Input("datepicker", "start_date"),
        Input("datepicker", "end_date"),
        Input("groupby_control", "value"),
        Input("climate_picker", "value"),
    ],
)
def make_generation_figure(country_control, start_date, end_date, group, climate_sel):
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    generation = dm.generation(country_control, Filter(start, end, group))
    del generation['country']

    desc = 'avg energy generation by production kind in GW'

    unit = 'GW'
    generation /= 1e3
    if climate_sel in climate.columns:
        generation = generation*climate[climate_sel]
        unit = 'tons'
        desc = climate_sel+' in '+unit

    generation = generation.fillna(value=0)
    generation = generation.loc[:, (generation != 0).any(axis=0)]
    g = generation.melt(
        var_name='kind', value_name='value', ignore_index=False)

    if g.empty:
        return dict(data=[], layout=dict(title="No Data Found for current interval"))

    figure = px.area(g, x=g.index, y="value", color='kind',
                     color_discrete_map=color_map, line_group="kind", 
                     category_orders={'kind': color_map.keys()})
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
        Input("datepicker", "start_date"),
        Input("datepicker", "end_date"),
        Input("groupby_control", "value"),
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
                      # xaxis_title=group,
                      yaxis_title='avg Exported to neighbour - imported in GWh',
                      hovermode="closest",
                      showlegend=True,
                      legend=dict(font=dict(size=10), orientation="h"),)
    fig.update_yaxes(ticksuffix=' GW')
    return fig


if __name__ == "__main__":
    app.layout = layout
    app.run_server(debug=True, use_reloader=True, host='0.0.0.0', port=8051)
