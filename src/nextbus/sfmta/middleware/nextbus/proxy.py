import xml.etree.ElementTree as ET
import csv
from datetime import datetime
import json
import os
import sys
import time
from typing import Any

import argh
import requests


URL_TMPL = "https://mw-test-transit-pavnextbusproxy-apim.azure-api.net/service/publicXMLFeed?command=predictions&a=sf-muni&stopId={stop_code}"

fields = {
    'agency_id': lambda preds, pred: preds['agency']['id'],
    'response_date': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%Y%m%d"),
    'response_time': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%H:%M:%S"),
    'stop_code': lambda preds, pred: preds['stop']['code'],
    'stop_name': lambda preds, pred: preds['stop']['name'],
    'route_id': lambda preds, pred: preds['route']['id'],

    'arrival_minutes': lambda preds, pred: pred['minutes'],
    'arrival_timestamp': lambda preds, pred: pred['timestamp'],
    'arrival_seconds': None,
    'direction_id': lambda preds, pred: pred['direction']['id'],
    'direction_name': lambda preds, pred: pred['direction']['name'],
    'trip_id': lambda preds, pred: pred['tripId'],
    'vehicle_id': lambda preds, pred: pred['vehicleId'],
    'block': None,
}

@argh.arg('stop_code', help='The stop code to request predictions for, e.g. 15419')
@argh.arg('--every-n-seconds', '-e', help='Request predictions every N seconds')
@argh.arg('--response-data-path', help='Name of the file to write raw response data to')
@argh.arg('--start-at', help='Time to start at, in YYYYmmddTHHMMSS format')
def main(stop_code: str, every_n_seconds: int = 60.0, response_data_path: str = None, start_at: str = None):
    url = URL_TMPL.format(stop_code=stop_code)

    writer = csv.writer(sys.stdout, dialect='excel')
    writer.writerow([header.replace('_', ' ') for header in fields.keys()])

    if response_data_path is None:
        response_data_path = f"responses_{datetime.now().strftime('%Y%m%dT%H%M%S')}_proxy.xml"
    raw_f = open(response_data_path, 'w')

    now = datetime.now()
    start_at_date = now
    if start_at is not None:
        start_at_date = datetime.strptime(start_at, "%Y%m%dT%H%M%S")

    time.sleep((start_at_date-now).total_seconds())

    while True:
        resp = requests.get(url)
        raw_f.write(resp.text)
        raw_f.flush()

        tree = ET.fromstring(resp.text)
        prediction = {}

        for tag in tree.iter():

            if tag.tag == 'predictions':
                prediction['route_id'] = tag.attrib['routeTag']
                prediction['stop_name'] = tag.attrib['stopTitle']
                continue

            if tag.tag == 'direction':
                prediction['direction_name'] = tag.attrib['title']
                continue

            if tag.tag == 'prediction':
                _prediction = {k: v for k, v in tag.items()}
                response_datetime = datetime.strptime(resp.headers['Date'], "%a, %d %b %Y %H:%M:%S %Z")
                prediction['response_date'] = response_datetime.strftime("%Y%m%d") 
                prediction['response_time'] = response_datetime.strftime("%H:%M:%S")
                prediction['agency_id'] = 'sfmta-cis'
                prediction['stop_code'] = stop_code
                prediction['arrival_minutes'] = _prediction['minutes']
                prediction['arrival_timestamp'] = _prediction['epochTime']
                prediction['arrival_seconds'] = _prediction['seconds']
                prediction['direction_id'] = _prediction['dirTag']
                prediction['trip_id'] = _prediction['tripTag']
                prediction['vehicle_id'] = _prediction['vehicle']
                prediction['block'] = _prediction['block']

                prediction = {k: prediction[k] for k in fields.keys()}

                writer.writerow(prediction.values())
                sys.stdout.flush()

        time.sleep(every_n_seconds - ((datetime.now() - start_at_date).total_seconds() % every_n_seconds))


if __name__ == '__main__':
    argh.dispatch_command(main)
