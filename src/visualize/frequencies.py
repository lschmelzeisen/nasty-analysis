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

import csv
from datetime import date, timedelta
from time import time
from typing import Counter, Dict, Mapping

from nasty import SearchFilter
from typing_extensions import Final

from src.config import (
    DAY_RESOLUTION,
    END_DATE,
    FILTERS,
    LANGUAGES,
    QUERIES,
    START_DATE,
    TOP_K_MOST_FREQUENT_WORDS,
    frequencies_file,
)

frequencies: Final[
    Dict[SearchFilter, Mapping[str, Mapping[str, Mapping[date, Counter[str]]]]]
] = {}


def load_frequencies() -> None:
    print("Loading frequencies...")
    time_before = time()

    for filter_ in FILTERS:
        frequencies_by_language = {}
        for language in LANGUAGES:
            frequencies_by_query = {}
            for query in QUERIES:
                frequencies_by_date = {}

                for days in range((END_DATE - START_DATE).days):
                    current_date = START_DATE + timedelta(days=days)
                    index_date = START_DATE + timedelta(
                        days=days - days % DAY_RESOLUTION
                    )
                    if days % DAY_RESOLUTION == 0:
                        frequencies_by_date[index_date] = Counter[str]()
                    frequencies_of_day = frequencies_by_date[index_date]

                    file = frequencies_file(filter_, language, query, current_date)
                    if not file.exists():
                        continue

                    with file.open(encoding="UTF-8", newline="") as fin:
                        csv_reader = csv.reader(fin)
                        for i, (word, frequency) in enumerate(csv_reader):
                            if len(word) < 3:
                                continue
                            frequencies_of_day[word] += int(frequency)
                            if i == TOP_K_MOST_FREQUENT_WORDS:
                                break

                frequencies_by_query[query] = frequencies_by_date
            frequencies_by_language[language] = frequencies_by_query
        frequencies[filter_] = frequencies_by_language

    time_after = time()
    print("  Took {:.2f}s.".format(time_after - time_before))
