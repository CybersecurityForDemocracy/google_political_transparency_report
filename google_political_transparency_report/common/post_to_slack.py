from os import environ
import requests
import json

def info_to_slack(msg):
    if environ.get("SLACKWH"):
        requests.post(environ.get("SLACKWH"), data=json.dumps({"text": msg}), headers={"Content-Type": "application/json"})

def post_to_slack(msg):
    msg += "\n post_to_slack is DEPRECATED, please fix this."
    info_to_slack(msg)

def warn_to_slack(msg):
    if environ.get("SLACKWARNWH"):
        requests.post(environ.get("SLACKWARNWH"), data=json.dumps({"text": '🚨 <@USBJ9RKAM>' + msg}), headers={"Content-Type": "application/json"})
