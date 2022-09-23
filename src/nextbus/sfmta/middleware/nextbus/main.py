import csv
from datetime import datetime
import json
import os
import sys
import time
from typing import Any

import argh
import requests


UMOIQ_URL_TMPL = "https://webservices.umoiq.com/api/pub/v1/agencies/{agency}/stopcodes/{stop_code}/predictions?key={umoiq_api_key}"
UMOIQ_API_KEY = os.getenv("UMOIQ_API_KEY")


fields = {
    'agency_id': lambda preds, pred: preds['agency']['id'],
    'response_date': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%Y%m%d"),
    'response_time': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%H:%M:%S"),
    'stop_code': lambda preds, pred: preds['stop']['code'],
    'stop_name': lambda preds, pred: preds['stop']['name'],
    'route_id': lambda preds, pred: preds['route']['id'],

    'arrival_minutes': lambda preds, pred: pred['minutes'],
    'arrival_timestamp': lambda preds, pred: pred['timestamp'],
    'direction_id': lambda preds, pred: pred['direction']['id'],
    'direction_name': lambda preds, pred: pred['direction']['name'],
    'trip_id': lambda preds, pred: pred['tripId'],
    'min_vehicle_id': lambda preds, pred: min(pred['linkedVehicleIds'].split(',')),
    'vehicle_id': lambda preds, pred: pred['vehicleId'],
    'linked_vehicle_ids': lambda preds, pred: pred['linkedVehicleIds'],
}


def parse_predictions(response_data: dict[str, Any]) -> list[dict[str, str]]:

    predictions = []
    for _predictions in response_data:

        for _prediction in _predictions['values']:
            prediction = {k: v(_predictions, _prediction) for k, v in fields.items()}
            predictions.append(prediction)
    return sorted(predictions, key=lambda pred: (pred['route_id'], pred['direction_name'], pred['vehicle_id']))
    

@argh.arg('stop_code', help='The stop code to request predictions for, e.g. 15419')
@argh.arg('--every-n-seconds', '-e', help='Request predictions every N seconds')
@argh.arg('--agency', help='The UmoIQ agency identifier to request predictions for')
@argh.arg('--response-data-path', help='Name of the file to write raw response data to')
@argh.arg('--start-at', help='Time to start at, in YYYYmmddTHHMMSS format')
def main(stop_code: str, every_n_seconds: int = 60.0, agency: str = 'sf-muni', response_data_path: str = None, start_at: str = None):
    umoiq_url = UMOIQ_URL_TMPL.format(stop_code=stop_code, umoiq_api_key=UMOIQ_API_KEY, agency=agency)

    writer = csv.writer(sys.stdout, dialect='excel')
    writer.writerow([header.replace('_', ' ') for header in fields.keys()])

    if response_data_path is None:
        response_data_path = f"responses_{datetime.now().strftime('%Y%m%dT%H%M%S')}_{agency}.json"
    raw_f = open(response_data_path, 'w')

    if start_at is not None:
        start_at_date = datetime.strptime(start_at, "%Y%m%dT%H%M%S")

    now = datetime.now()
    time.sleep((start_at_date-now).total_seconds())

    while True:
        resp = requests.get(umoiq_url)

        response_data = resp.json()
        raw_f.write(json.dumps(response_data))
        raw_f.flush()

        predictions = parse_predictions(response_data)
        
        for prediction in predictions:
            writer.writerow(prediction.values())
        sys.stdout.flush()

        time.sleep(every_n_seconds - ((datetime.now() - start_at_date).total_seconds() % every_n_seconds))


if __name__ == '__main__':
    argh.dispatch_command(main)
