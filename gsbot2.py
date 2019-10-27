# imports for Slack
from slackeventsapi import SlackEventAdapter
from slackclient import SlackClient
import requests
import os

# imports for Goldman Sachs
from datetime import date
from gs_quant.data import Dataset
from gs_quant.markets.securities import SecurityMaster, AssetIdentifier
from gs_quant.session import GsSession
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import six

# Our app's Slack Event Adapter for receiving actions via the Events API
slack_signing_secret = '44859eaa96cc7e60035d2592136cd3a4'
slack_events_adapter = SlackEventAdapter(slack_signing_secret, "/slack/events")

# Create a SlackClient for the bot to use for Web API requests
slack_bot_token = 'xoxb-810243636740-799237632115-OvDWjd1vXiWvN7g91bztET0P'
slack_client = SlackClient(slack_bot_token)

# global string for remember the last response
response = ""

# global string for remember the last response
data = pd.DataFrame()

# global string for remember the last response
num_rows = 5

# Auth details for GS
client_id = '64019d2ee21e4a04b6722c5ee4f3bd7d'
client_secret = '2d4b5d81d3b7b114f90cf0462bedc581c119242a53240afbb080b09fd3bd1f40'



# function for generating table png
def render_mpl_table(dataf, num_rows=5 ,col_width=3.0, row_height=0.625, font_size=14,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    data = dataf.head(num_rows)
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax

# Main Handler for Communication
@slack_events_adapter.on("app_mention")
def handle_mention(event_data):
    global response
    global data
    global num_rows
    message = event_data["event"]
    text_l = message.get('text').lower()
    channel = message["channel"]
    # Login to Marquee
    scopes = GsSession.Scopes.get_default()
    GsSession.use(client_id=client_id, client_secret=client_secret, scopes=scopes)
    # retrieve data
    ds = Dataset('USCANFPP_MINI')

    # get a list of covered GSIDs
    gsids = ds.get_coverage()['gsid'].values.tolist()
    if message.get("subtype") is None:
        if "hi" in text_l or "hello" in text_l:
            user_info = requests.get("https://slack.com/api/users.info?token=" + slack_bot_token + "&user=" + message["user"] + "&pretty=1").json()
            user_profile = user_info["user"]["profile"]
            user_first_name = user_profile["first_name"]
            response = "Hello, " + user_first_name + ", how are you doing?"
            slack_client.api_call("chat.postMessage", channel=channel, text=response)
        elif "how about you" in text_l:
            response = "I'm good."
            slack_client.api_call("chat.postMessage", channel=channel, text=response)
        elif "what?" in text_l or "what did you just say" in text_l:
            response = "I said: \"" + response + "\""
            slack_client.api_call("chat.postMessage", channel=channel, text=response)
        elif "start date" in text_l and "end date" in text_l and "gsid" in text_l:
            text_list = text_l.split(',')
            text_dict = {}
            for section in text_list:
                pair = section.split(':')
                for i in range(2):
                    pair[i] = pair[i].strip()
                if "gsid" not in section:
                    if "start date" in pair[0]:
                        pair[0] = "start date"
                    text_dict[pair[0]] = [int(num) for num in pair[1].split('/')]
                else:
                    text_dict[pair[0]] = int(pair[1])
            print("!!!:", text_dict)
            data = ds.get_data(date(text_dict["start date"][2], text_dict["start date"][0], text_dict["start date"][1]),
                               date(text_dict["end date"][2], text_dict["end date"][0], text_dict["end date"][1]),
                               gsid=gsids[0:text_dict["gsid"]])
            response = "I've prepared a table image for you. It has " + str(len(data)) + " rows since it corresponds to that many days. Which row would you like to see? If multiple rows please type \"multiple rows: start_row end_row\"."
            slack_client.api_call("chat.postMessage", channel=channel, text=response)
        elif "multiple rows" in text_l:
            start_row = int(text_l.split(' ')[3].strip())
            end_row = int(text_l.split(' ')[4].strip())
            try:
                #barc = data.plot.bar(x=0, y=y_index, rot=0)
                #barc.get_figure().savefig("barchart.png")
                #slack_client.api_call("files.upload", channel=channel, file="barchart.png")
                response = str(data.loc[start_row:end_row, :].to_dict())
                slack_client.api_call("chat.postMessage", channel=channel, text=response)
            except Exception as e:
                slack_client.api_call("chat.postMessage", channel=channel, text="Whoops! I can't generate the image for you. Make sure the number of rows is appropriate!")
        elif "row" in text_l:
            num_row = int(text_l.split(' ')[2].strip())
            try:
                #render_mpl_table(data, num_rows=num_rows, header_columns=0, col_width=2.0).get_figure().savefig("table.png")
                #slack_client.api_call("files.upload", channel=channel, file="table.png")
                response = str(data.iloc[num_row].to_dict())
                slack_client.api_call("chat.postMessage", channel=channel, text=response)
            except Exception as e:
                slack_client.api_call("chat.postMessage", channel=channel, text="Whoops! I can't generate the row for you. Make sure the row number is appropriate!")
        


            


@slack_events_adapter.on("app_home_opened")
def handle_home_open(event_data):
    message = event_data["event"]
    channel = message["channel"]
    response = "Please remember to @ me, or else I won't respond! :tada:"
    slack_client.api_call("chat.postMessage", channel=channel, text=response)


# Example reaction emoji echo
@slack_events_adapter.on("reaction_added")
def reaction_added(event_data):
    event = event_data["event"]
    emoji = event["reaction"]
    channel = event["item"]["channel"]
    text = ":%s:" % emoji
    slack_client.api_call("chat.postMessage", channel=channel, text=text)

# Error events
@slack_events_adapter.on("error")
def error_handler(err):
    print("ERROR: " + str(err))

# Once we have our event listeners configured, we can start the
# Flask server with the default `/events` endpoint on port 3000
slack_events_adapter.start(port=3000)