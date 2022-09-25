import csv
from datetime import datetime
from email.utils import parsedate_to_datetime
import json
import os
import sys
import time
from typing import Any

import argh
import requests


UMOIQ_URL_TMPL = "https://webservices.umoiq.com/api/pub/v1/agencies/{agency}/stopcodes/{stop_code}/predictions?key={umoiq_api_key}"
UMOIQ_API_KEY = os.getenv("UMOIQ_API_KEY")


computed_fields = {
    'agency_id': lambda preds, pred: preds['agency']['id'],
    'response_date': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%Y%m%d"),
    'response_time': lambda preds, pred: datetime.fromtimestamp(preds['serverTimestamp'] / 1000.0).strftime("%H:%M:%S"),
    'response_timezone': lambda preds, pred: "UTC",
    'stop_code': lambda preds, pred: preds['stop']['code'],
    'stop_name': lambda preds, pred: preds['stop']['name'],
    'route_id': lambda preds, pred: preds['route']['id'],

    'arrival_seconds': lambda preds, pred: round((datetime.fromtimestamp(pred['timestamp'] / 1000.0) - datetime.now()).total_seconds()),
    'arrival_minutes': lambda preds, pred: pred['minutes'],
    'arrival_epoch': lambda preds, pred: pred['timestamp'],
    'direction_id': lambda preds, pred: pred['direction']['id'],
    'direction_name': lambda preds, pred: pred['direction']['name'],
    'trip_id': lambda preds, pred: pred['tripId'],
    'min_vehicle_id': lambda preds, pred: min(pred['linkedVehicleIds'].split(',')),
    'vehicle_id': lambda preds, pred: pred['vehicleId'],
    'linked_vehicle_ids': lambda preds, pred: pred['linkedVehicleIds'],
}

field_order = [
    'agency_id',
    'request_date',
    'request_time',
    'request_timezone',
    'response_date',
    'response_time',
    'response_timezone',
    'response_duration',

    'stop_code',
    'stop_name',
    'route_id',

    'arrival_seconds',
    'arrival_minutes',
    'arrival_epoch',

    'direction_id',
    'direction_name',
    'trip_id',
    'min_vehicle_id',

    'vehicle_id',
    'linked_vehicle_ids',
]


def parse_predictions(response_data: dict[str, Any]) -> list[dict[str, str]]:

    predictions = []
    for _predictions in response_data:

        for _prediction in _predictions['values']:
            prediction = {k: v(_predictions, _prediction) for k, v in computed_fields.items()}
            predictions.append(prediction)
    return sorted(predictions, key=lambda pred: (pred['route_id'], pred['direction_name'], pred['vehicle_id']))
    

@argh.arg('stop_code', help='The stop code to request predictions for, e.g. 15419')
@argh.arg('--every-n-seconds', '-e', help='Request predictions every N seconds')
@argh.arg('--agency', help='The UmoIQ agency identifier to request predictions for')
@argh.arg('--response-data-path', help='Name of the file to write raw response data to')
@argh.arg('--start-at', help='Time to start at, in rfc-3339 format e.g. 2022-09-25 15:17:34-07:00')
def main(stop_code: str, every_n_seconds: int = 60.0, agency: str = 'sf-muni', response_data_path: str = None, start_at: str = None):
    umoiq_url = UMOIQ_URL_TMPL.format(stop_code=stop_code, umoiq_api_key=UMOIQ_API_KEY, agency=agency)

    writer = csv.writer(sys.stdout, dialect='excel')
    writer.writerow([header.replace('_', ' ') for header in field_order])

    if response_data_path is None:
        response_data_path = f"responses_{datetime.now().strftime('%Y%m%dT%H%M%S')}_{agency}.json"
    raw_f = open(response_data_path, 'w')

    # use the time zone of the given start datetime, if given
    # otherwise, default to the local timezone of this process
    start_at_date = None
    if start_at is not None:
        start_at_date = datetime.fromisoformat(start_at)

    timezone = datetime.now().astimezone().tzinfo if start_at_date is None else start_at_date.tzinfo
    now = datetime.now(tz=timezone)
    start_at_date = now if start_at_date is None else start_at_date

    time.sleep((start_at_date-now).total_seconds())

    while True:
        now = datetime.now(tz=timezone)
        resp = requests.get(umoiq_url)

        if not resp.ok:
            print(json.dumps({'status_code': resp.status_code, 'url': resp.url, 'errorMessage': resp.errorMessage, 'reason': resp.reason, 'elapsed': resp.elapsed.total_seconds()}), file=sys.stderr)

        else:
            response_data = resp.json()
            raw_f.write(json.dumps(response_data))
            raw_f.flush()

            response_datetime = parsedate_to_datetime(resp.headers['Date'])
            predictions = parse_predictions(response_data)
            predictions = [{
                **prediction,
                **{
                    'request_date': now.strftime("%Y%m%d"),
                    'request_time': now.strftime("%H:%M:%S"),
                    'request_timezone': now.tzname(),
                    'response_duration': resp.elapsed.total_seconds(),}
            } for prediction in predictions]

            predictions_w_sorted_keys = [
                {k: prediction[k] for k in field_order}
                for prediction in predictions
            ]

            for prediction in predictions_w_sorted_keys:
                writer.writerow(prediction.values())
            sys.stdout.flush()

        time.sleep(every_n_seconds - ((datetime.now(tz=timezone) - start_at_date).total_seconds() % every_n_seconds))


if __name__ == '__main__':
    argh.dispatch_command(main)
