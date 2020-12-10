import dash
import dash_daq as daq
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, ClientsideFunction
import plotly.graph_objs as go
import pandas as pd
import os
import requests
import json
import datetime as dt
import pickle
from dataset import Dataset

app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

data_obj = Dataset()
if os.path.exists('data/cases-india.csv') == False and\
os.path.exists('data/cases-states.csv') == False:
    data_obj.download()
else:
    data_obj.update()

df = pd.read_csv('India-cases.csv')
df_states = pd.read_csv('data/cases-states.csv')
df_india = pd.read_csv('data/cases-india.csv')
states = ['India']
states += df_states.state.unique().tolist()

def generate_top():
    df = pd.read_csv('data/latest-data.csv')
    cols = ['active_cum','recovered_cum','deceased_cum','confirmed_cum',
            'active_cum_diff','recovered_cum_diff','deceased_cum_diff',
            'confirmed_cum_diff']
    col_names = ['Active', 'Recovered', 'Deceased', 'Confirmed']
    class_names = ['active','recovered','deceased','confirmed']
    df = df.loc[df['state']=='India',:]
    rows = []
    for i in range(4):
        arrow = '&#8595;' if (i==0) and (df[cols[4]].iloc[0] > 0) else '&#8593;'
        rows.append(html.Th([
            html.P(col_names[i]),
            html.H4(df[cols[i]].iloc[0],className=class_names[i]),
            dcc.Markdown(f"{arrow} {df[cols[i+4]].iloc[0]}",
                className=class_names[i])
        ]))
    return html.Table([
        html.Thead(rows)
    ], id='top-details')


def get_title(data, state='India', is_cum=True, col='active'):
    if data:
        curr = int(data['points'][0]['y'])
        date = data['points'][0]['x']
        if state == 'India':
            df = df_india[df_india['dateymd']==date]
        else:
            df = df_states[(df_states['dateymd']==date) \
                            & (df_states['state']==state)]
        if is_cum:
            diff = df[f"{col}_cum_diff"].iloc[0]
            perc = df[f"{col}_cum_perc"].iloc[0]
        else:
            diff = df[f"{col}_diff"].iloc[0]
            perc = df[f"{col}_perc"].iloc[0]
        date = pd.to_datetime(date)
        date = date.strftime("%a %b %d %Y")
        return f"<b>{curr}\t{diff} ({perc}%)</b><br><span style='color:#4A4A4A'>{date}</span>"
    return None

def generate_table(df, max_rows=10):
    _, date = data_obj.get_last_updated_date()
    table = df[df['dateymd']==date]
    table.sort_values('confirmed_cum',ascending=False,inplace=True)
    table.reset_index(drop=True,inplace=True)
    table_cols = ['Active','Recovered','Deceased','Confirmed']
    first_cols = ['active_cum','recovered_cum','deceased_cum','confirmed_cum']
    table['dummy'] = None
    second_cols = ['dummy','recovered_cum_diff','deceased_cum_diff',
                   'confirmed_cum_diff']
    class_names = ['active','recovered','deceased','confirmed']
    rows = []
    for i in range(min(len(table),max_rows)):
        rows.append(html.Tr([
            html.Td(table.loc[i,'state'], style={'paddingLeft':'20px'}),
            html.Td(None),html.Td(None),html.Td(None)
        ], style={'backgroundColor':'#E7E8E9','fontWeight': 'bold'}))
        rows.append(html.Tr([
            html.Td(table.loc[i,col],className=class_names[j])\
            for j,col in enumerate(first_cols)
        ]))
        rows.append(html.Tr([
            html.Td(
                dcc.Markdown(f"&#8593; {table.loc[i,col]}")\
                if col != 'dummy' else dcc.Markdown(table.loc[i,col]),
                className=class_names[j]) for j,col in enumerate(second_cols)
        ]))

    return html.Table([
        html.Thead(
            html.Tr([html.Th(col,className=class_names[i])\
             for i,col in enumerate(table_cols)])
        ),
        html.Tbody(rows)
    ], className='grid')


app.layout = html.Div([
    html.Table([
        html.Thead([
        html.Th(html.H1('COVID19 India Dashboard')),
        html.Th(html.A(
            html.Button("About me", id='about-me-button'),
            href="https://www.kaggle.com/anshuls235")
            )
        ])
    ], id='header-table'),
    html.Br(),
    html.Div(
            generate_top()
        ),
    html.Br(),
    dcc.Dropdown(
            id = 'state-dropdown',
            options=[{'label':state,'value':state} for state in states],
            value='India'
    ),
    html.Br(),
    html.Div([
        dcc.Tabs(id="tabs", value='active', children=[
            dcc.Tab(label='Active', value='active'),
            dcc.Tab(label='Confirmed', value='confirmed'),
            dcc.Tab(label='Recovered', value='recoveries'),
            dcc.Tab(label='Deceased', value='deaths'),
        ]),
    dcc.Graph(id='india-graph'),
    html.Br(),
    html.Div([
        daq.ToggleSwitch(
            id='my-toggle-switch',
            label = ['Daily','Cumulative'],
            value=True
            ),
    ], id='toggle-switch-div'),
    html.Br()
    ], id='tabs_plus_graph'),
    html.Br(),
    html.H3('All State/UT Stats'),
    html.Br(),
    generate_table(df_states,max_rows=40),

], id='main-div')

app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("count_graph", "figure")],
)

@app.callback(Output('india-graph','figure'),
              [Input('state-dropdown','value'),
              Input('india-graph','hoverData'),
              Input('my-toggle-switch','value'),
              Input('tabs','value')])
def update_tab_graph(state,hdata,r_value,col):

    col_map = {
        'active':'active',
        'confirmed': 'confirmed',
        'recoveries': 'recovered',
        'deaths': 'deceased'
    }

    data_color_dict = {
        'active': '#F06372',
        'confirmed': '#F06372',
        'recoveries': '#249895',
        'deaths': '#4A4A4A'
    }

    layout_dict = {
        'active': {
            'paper_bgcolor': '#FCDFE3',
            'plot_bgcolor': '#FCDFE3'
        },
        'confirmed': {
            'paper_bgcolor': '#FCDFE3',
            'plot_bgcolor': '#FCDFE3'
        },
        'recoveries': {
            'paper_bgcolor': '#EFF8F7',
            'plot_bgcolor': '#EFF8F7'
        },
        'deaths': {
            'paper_bgcolor': '#F6F6F6',
            'plot_bgcolor': '#F6F6F6'
        }
    }
    if state == 'India':
        df_state = df_india
    else:
        df_state = df_states.loc[df_states['state']==state,:]

    if r_value == False:
        data = [go.Bar(x=df_state['dateymd'],
                y=df_state[col_map.get(col,None)],
                marker_color=data_color_dict.get(col,None))]
    else:
        data = [go.Scatter(x=df_state['dateymd'],
                y=df_state[f"{col_map.get(col,None)}_cum"],
                mode='lines',
                marker_color=data_color_dict.get(col,None),
                line_width=5)]

    layout_kwargs = layout_dict.get(col,None)
    layout = go.Layout(template = 'plotly_white',
                       title = get_title(hdata, state, r_value,
                                        col_map.get(col,None)),
                       title_font_color = data_color_dict.get(col,None),
                       title_font_size = 20,
                       hovermode='x',
                       xaxis = dict(
                        tickfont_color = '#B5A1A4',
                        linecolor = 'black',
                        showgrid = False,
                        zeroline = False),
                       yaxis = dict(
                        nticks = 10,
                        zeroline = False,
                        tickfont_color = '#B5A1A4',
                        gridcolor = '#B5A1A4'),
                        **layout_kwargs)

    return {
        'data': data,
        'layout': layout
    }

if __name__ == '__main__':
    app.run_server()
