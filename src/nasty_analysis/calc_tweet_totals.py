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

import json
from typing import Iterator, cast

from nasty import BatchEntry, BatchResults, Search
from tqdm import tqdm

from nasty_analysis.config import CRAWL_DIR, TOTALS_FILE

twitter_crawl = BatchResults(CRAWL_DIR)
with TOTALS_FILE.open("w", encoding="UTF-8") as fout:
    for entry in tqdm(
        cast(Iterator[BatchEntry], twitter_crawl), desc="Twitter-Crawl Entries"
    ):
        assert isinstance(entry.request, Search)
        assert entry.request.since is not None
        fout.write(
            json.dumps(
                {
                    "filter": str(entry.request.filter.to_json()),
                    "lang": entry.request.lang,
                    "query": entry.request.query,
                    "since": entry.request.since.strftime("%Y-%m-%d"),
                    "total": str(sum(1 for _ in twitter_crawl.tweets(entry))),
                }
            )
        )
        fout.write("\n")
