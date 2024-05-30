import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from jupyter_dash import JupyterDash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import random
import requests
import json
from pymongo import MongoClient

client = MongoClient("mongodb+srv://nvkhoa14:UITHKT@cluster0.irat42q.mongodb.net")
db = client.lab4
coll = db.tweet_sentiment

def counting(coll):
    a = coll.count_documents({'sentiment':"NEU"})
    b = coll.count_documents({'sentiment':"POS"})
    c = coll.count_documents({'sentiment':"NEG"})
    return [a,b,c]


res = counting(coll)
# res = requests.get("http://localhost:2703/getData")
# res = json.loads(res.text)
pd.options.plotting.backend = "plotly"
countdown = 20

np.random.seed(4); cols = list('abc')
X = np.random.randn(1,len(cols))  
df= pd.DataFrame([['neu',res[0]],['pos',res[1]],['neg',res[2]]],columns=['type','amount'])# plotly figure
fig = px.bar(df,x='type',y='amount')

app = JupyterDash(__name__)
app.layout = html.Div([
    html.H1("Streaming of Tweet Sentiment Data"),
            dcc.Interval(
            id='interval-component',
            interval=2*1000, # in milliseconds
            n_intervals=0
        ),
    dcc.Graph(id='graph'),
])

@app.callback(
    Output('graph', 'figure'),
    [Input('interval-component', "n_intervals")]
)
def streamFig(value):
    global df
    global coll
        
    res = counting(coll)
    # res = requests.get("http://localhost:2703/getData")
    # res = json.loads(res.text)

    df['amount'][0] = res[0]
    df['amount'][1] = res[1]
    df['amount'][2] = res[2]
    df3 = df.copy()
    fig = px.bar(df3,x='type',y='amount')
    
    return(fig)

app.run_server(mode='external', port = 8069, dev_tools_ui=True, #debug=True,
              dev_tools_hot_reload =True, threaded=True)
