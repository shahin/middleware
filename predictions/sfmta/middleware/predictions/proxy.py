import xml.etree.ElementTree as ET
import csv
from datetime import datetime
from email.utils import parsedate_to_datetime
import json
import sys
import time

import argh
import requests


URL_TMPL = "https://mw-test-transit-pavnextbusproxy-apim.azure-api.net/service/publicXMLFeed?command=predictions&a=sf-muni&stopId={stop_code}"  # noqa: E501

fields = [
    "agency_id",
    "request_date",
    "request_time",
    "request_timezone",
    "response_date",
    "response_time",
    "response_timezone",
    "response_duration",
    "stop_code",
    "stop_name",
    "route_id",
    "arrival_seconds",
    "arrival_minutes",
    "arrival_epoch",
    "direction_id",
    "direction_name",
    "trip_id",
    "vehicle_id",
    "block",
]


@argh.arg("stop_code", help="The stop code to request predictions for, e.g. 15419")
@argh.arg("--every-n-seconds", "-e", help="Request predictions every N seconds")
@argh.arg("--response-data-path", help="Name of the file to write raw response data to")
@argh.arg(
    "--start-at",
    help="Time to start at, in rfc-3339 format e.g. 2022-09-25 15:17:34-07:00",
)
def main(
    stop_code: str,
    every_n_seconds: int = 60.0,
    response_data_path: str = None,
    start_at: str = None,
):
    url = URL_TMPL.format(stop_code=stop_code)

    writer = csv.writer(sys.stdout, dialect="excel")
    writer.writerow([header.replace("_", " ") for header in fields])

    if response_data_path is None:
        response_data_path = (
            f"responses_{datetime.now().strftime('%Y%m%dT%H%M%S')}_proxy.xml"
        )
    raw_f = open(response_data_path, "w")

    # use the time zone of the given start datetime, if given
    # otherwise, default to the local timezone of this process
    start_at_date = None
    if start_at is not None:
        start_at_date = datetime.fromisoformat(start_at)
    timezone = (
        datetime.now().astimezone().tzinfo
        if start_at_date is None
        else start_at_date.tzinfo
    )
    now = datetime.now(tz=timezone)
    start_at_date = now if start_at_date is None else start_at_date

    time.sleep((start_at_date - now).total_seconds())

    while True:
        now = datetime.now(tz=timezone)
        resp = requests.get(url)

        if not resp.ok:
            print(
                json.dumps(
                    {
                        "status_code": resp.status_code,
                        "url": resp.url,
                        "errorMessage": resp.errorMessage,
                        "reason": resp.reason,
                        "elapsed": resp.elapsed.total_seconds(),
                    }
                ),
                file=sys.stderr,
            )

        raw_f.write(resp.text)
        raw_f.flush()

        tree = ET.fromstring(resp.text)
        prediction = {}

        for tag in tree.iter():

            if tag.tag == "predictions":
                prediction["route_id"] = tag.attrib["routeTag"]
                prediction["stop_name"] = tag.attrib["stopTitle"]
                continue

            if tag.tag == "direction":
                prediction["direction_name"] = tag.attrib["title"]
                continue

            if tag.tag == "prediction":
                _prediction = {k: v for k, v in tag.items()}
                response_datetime = parsedate_to_datetime(resp.headers["Date"])
                prediction["request_date"] = now.strftime("%Y%m%d")
                prediction["request_time"] = now.strftime("%H:%M:%S")
                prediction["request_timezone"] = now.tzname()
                prediction["response_date"] = response_datetime.strftime("%Y%m%d")
                prediction["response_time"] = response_datetime.strftime("%H:%M:%S")
                prediction["response_timezone"] = response_datetime.tzname()
                prediction["response_duration"] = resp.elapsed.total_seconds()
                prediction["agency_id"] = "sfmta-cis"
                prediction["stop_code"] = stop_code
                prediction["arrival_seconds"] = _prediction["seconds"]
                prediction["arrival_minutes"] = _prediction["minutes"]
                prediction["arrival_epoch"] = _prediction["epochTime"]
                prediction["direction_id"] = _prediction["dirTag"]
                prediction["trip_id"] = _prediction["tripTag"]
                prediction["vehicle_id"] = _prediction["vehicle"]
                prediction["block"] = _prediction["block"]

                prediction = {k: prediction[k] for k in fields}

                writer.writerow(prediction.values())
                sys.stdout.flush()

        time.sleep(
            every_n_seconds
            - (
                (datetime.now(tz=timezone) - start_at_date).total_seconds()
                % every_n_seconds
            )
        )


if __name__ == "__main__":
    argh.dispatch_command(main)
