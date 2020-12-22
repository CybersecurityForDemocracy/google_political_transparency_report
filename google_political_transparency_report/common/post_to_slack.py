from os import environ
import requests
import json

def post_to_slack(msg):
    if environ.get("SLACKWH"):
        requests.post(environ.get("SLACKWH"), data=json.dumps({"text": msg}), headers={"Content-Type": "application/json"})
