#
# Copyright 2020 Lukas Schmelzeisen
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

from datetime import date, timedelta
from pathlib import Path

from nasty import SearchFilter

QUERIES = [
    "corona",
    "coronavirus",
    "wuhan",
    "covid",
    "covid19",
    "sars",
    "ncov",
]
START_DATE = date(2019, 12, 1)
END_DATE = date(2020, 3, 1)
LANGUAGES = ["de", "en"]
LANGUAGES_NATURAL = {
    "de": "German",
    "en": "English",
}
FILTERS = [SearchFilter.TOP, SearchFilter.LATEST]

BATCH_FILE = Path("batch.jsonl")
CRAWL_DIR = Path("twitter-crawl")
CSV_DIR = Path("twitter-crawl.csv")
FREQUENCIES_DIR = Path("twitter-crawl.frequencies")


def csv_file(filter_: SearchFilter, language: str, query: str, date_: date) -> Path:
    return (
        CSV_DIR
        / filter_.name.lower()
        / language
        / "{}-{}.tweets.csv".format(query.replace(" ", "-"), date_)
    )


def frequencies_file(
    filter_: SearchFilter, language: str, query: str, date_: date
) -> Path:
    return (
        FREQUENCIES_DIR
        / filter_.name.lower()
        / language
        / "{}-{}.frequencies.csv".format(query.replace(" ", "-"), date_)
    )


# src/export_csv.py
CSV_FIELDS = [
    "url",
    "created_at",
    "reply_count",
    "retweet_count",
    "favorite_count",
    "full_text",
]

# src/visualize
DAY_RESOLUTION = 5
START_DATE_RESOLUTION = START_DATE
END_DATE_RESOLUTION = START_DATE + timedelta(
    days=(END_DATE - START_DATE).days - (END_DATE - START_DATE).days % DAY_RESOLUTION
)
TOP_K_MOST_FREQUENT_WORDS = 100
NUM_TREND_INPUTS = 5
