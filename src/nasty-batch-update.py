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

from datetime import date
from pathlib import Path

import nasty  # type: ignore

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
END_DATE = date(2020, 2, 25)
LANGUAGES = ["de", "en"]
FILTERS = [nasty.SearchFilter.TOP, nasty.SearchFilter.LATEST]


def is_request_in_batch(  # type: ignore
    request: nasty.Request, batch: nasty.Batch
) -> bool:
    for entry in batch:
        if request == entry.request:
            return True
    return False


batch_file = Path("batch.jsonl")

batch = nasty.Batch()
if batch_file.exists():
    batch.load(batch_file)

for language in LANGUAGES:
    for filter_ in FILTERS:
        for query in QUERIES:
            request = nasty.Search(
                query,
                since=START_DATE,
                until=END_DATE,
                filter_=filter_,
                lang=language,
                max_tweets=None,
                batch_size=100,
            )
            for daily_request in request.to_daily_requests():
                if is_request_in_batch(daily_request, batch):
                    continue

                batch.append(daily_request)

batch.dump(batch_file)
