#
# Copyright 2019-2020 Lukas Schmelzeisen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import csv
import json
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from logging import getLogger
from pathlib import Path
from time import sleep
from typing import AbstractSet, Iterator, Mapping, Sequence

import requests
from nasty_utils import ColoredBraceStyleAdapter, checked_cast, date_range
from requests import Session
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_TERMS = {"coronavirus", "corona", "covid", "covid19", "sars", "ncov"}
_DOMAINS = {"bild.de", "sueddeutsche.de", "faz.net"}
_START_DATE = date(2019, 12, 1)
_END_DATE = date(2020, 9, 15)

_MAX_MAXRECORDS_PER_QUERY = 250

_OUT_FILE = Path("gdelt.csv")


class GdeltArticle:
    def __init__(self, article_json: Mapping[str, object]):
        self.url = checked_cast(str, article_json["url"])
        self.url_mobile = checked_cast(str, article_json["url_mobile"])
        self.title = checked_cast(str, article_json["title"])
        try:
            self.seendate = datetime.strptime(
                checked_cast(str, article_json["seendate"]), "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            self.seendate = datetime.strptime(
                checked_cast(str, article_json["seendate"]), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
        self.socialimage = checked_cast(str, article_json["socialimage"])
        self.domain = checked_cast(str, article_json["domain"])
        self.language = checked_cast(str, article_json["language"])
        self.sourcecountry = checked_cast(str, article_json["sourcecountry"])

    @classmethod
    def fields(self) -> Sequence[str]:
        return [
            "url",
            "url_mobile",
            "title",
            "seendate",
            "socialimage",
            "domain",
            "language",
            "sourcecountry",
        ]

    def to_dict(self) -> Mapping[str, str]:
        return {
            "url": self.url,
            "url_mobile": self.url_mobile,
            "title": self.title,
            "seendate": self.seendate.strftime("%Y-%m-%d %H:%M:%S"),
            "socialimage": self.socialimage,
            "domain": self.domain,
            "language": self.language,
            "sourcecountry": self.sourcecountry,
        }


def gdelt() -> None:
    with requests.Session() as session, _OUT_FILE.open("w", encoding="UTF-8") as fout:
        csv_writer = csv.DictWriter(fout, fieldnames=GdeltArticle.fields())
        csv_writer.writeheader()

        # Configure on which status codes we should perform automated retries.
        session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=5,
                    connect=5,
                    redirect=10,
                    backoff_factor=0.1,
                    raise_on_redirect=True,
                    raise_on_status=True,
                    status_forcelist=[
                        HTTPStatus.REQUEST_TIMEOUT,  # HTTP 408
                        HTTPStatus.CONFLICT,  # HTTP 409
                        HTTPStatus.INTERNAL_SERVER_ERROR,  # HTTP 500
                        HTTPStatus.NOT_IMPLEMENTED,  # HTTP 501
                        HTTPStatus.BAD_GATEWAY,  # HTTP 502
                        HTTPStatus.SERVICE_UNAVAILABLE,  # HTTP 503
                        HTTPStatus.GATEWAY_TIMEOUT,  # HTTP 504
                    ],
                )
            ),
        )

        for day in tqdm(
            date_range(_START_DATE, _END_DATE),
            desc="Fetching articles lists",
            total=(_END_DATE - _START_DATE).days + 1,
        ):
            for domain in tqdm(_DOMAINS, desc="{:%Y-%m-%d}".format(day)):
                for article in _fetch_article_list_from_gdelt(
                    session, _TERMS, domain, day
                ):
                    csv_writer.writerow(article.to_dict())


# https://api.gdeltproject.org/api/v2/doc/doc?query=(corona%20OR%20coronavirus%20OR%20covid%20OR%20covid19%20OR%20sars%20OR%20ncov)%20domain:bild.de&mode=artlist&maxrecords=250&startdatetime=20200801000000&enddatetime=20200802000000&format=json


def _fetch_article_list_from_gdelt(
    session: Session,
    terms: AbstractSet[str],
    domain: str,
    day: date,
) -> Iterator[GdeltArticle]:
    query_str = "(" + " OR ".join(terms) + ")" if len(terms) > 1 else next(iter(terms))
    sleep(10)
    with requests.Session() as s:
        s.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=5,
                    connect=5,
                    redirect=10,
                    backoff_factor=0.1,
                    raise_on_redirect=True,
                    raise_on_status=True,
                    status_forcelist=[
                        HTTPStatus.REQUEST_TIMEOUT,  # HTTP 408
                        HTTPStatus.CONFLICT,  # HTTP 409
                        HTTPStatus.INTERNAL_SERVER_ERROR,  # HTTP 500
                        HTTPStatus.NOT_IMPLEMENTED,  # HTTP 501
                        HTTPStatus.BAD_GATEWAY,  # HTTP 502
                        HTTPStatus.SERVICE_UNAVAILABLE,  # HTTP 503
                        HTTPStatus.GATEWAY_TIMEOUT,  # HTTP 504
                    ],
                )
            ),
        )
        response = s.get(
            "https://api.gdeltproject.org/api/v2/doc/doc?",
            params={
                "query": query_str + " domain:{}".format(domain),
                "mode": "artlist",
                "maxrecords": _MAX_MAXRECORDS_PER_QUERY,
                "startdatetime": "{:%Y%m%d}000000".format(day),
                "enddatetime": "{:%Y%m%d}000000".format(day + timedelta(days=1)),
                "format": "json",
            },
        )

    if response.status_code != HTTPStatus.OK.value:
        raise Exception(
            "Unexpected status code {} for URL {}.".format(
                HTTPStatus(response.status_code), response.url
            )
        )

    response_json = json.loads(response.text)
    if not response_json:
        return []

    if len(response_json) == _MAX_MAXRECORDS_PER_QUERY:
        raise Exception("To many articles for GDELT maxrecords limit.")

    for article_json in response_json["articles"]:
        yield GdeltArticle(article_json)
