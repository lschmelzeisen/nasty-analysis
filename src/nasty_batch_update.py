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

from nasty import Batch, Request, Search

from src.config import BATCH_FILE, END_DATE, FILTERS, LANGUAGES, QUERIES, START_DATE


def is_request_in_batch(request: Request, batch: Batch) -> bool:
    for entry in batch:
        if request == entry.request:
            return True
    return False


batch = Batch()
if BATCH_FILE.exists():
    batch.load(BATCH_FILE)

for language in LANGUAGES:
    for filter_ in FILTERS:
        for query in QUERIES:
            request = Search(
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

batch.dump(BATCH_FILE)
