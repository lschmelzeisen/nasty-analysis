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
import json
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, cast

from nasty import SearchFilter

from src.config import TOTALS_FILE


# Adapted from https://stackoverflow.com/a/25470943/211404
def yyyy_mm_dd_date(string: str) -> date:
    try:
        return datetime.strptime(string, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            'Can not parse date: "{}". Make sure it is in '
            "YYYY-MM-DD format.".format(string)
        )


totals: Dict[SearchFilter, Dict[str, Dict[date, Dict[str, int]]]] = defaultdict(
    lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
)


with TOTALS_FILE.open(encoding="UTF-8") as fin:
    for line in fin:
        entry = json.loads(line)
        totals[SearchFilter.from_json(entry["filter"])][entry["lang"]][
            yyyy_mm_dd_date(entry["since"])
        ][entry["query"]] = int(entry["total"])

for filter_, totals_for_filter in totals.items():
    for lang, totals_for_lang in totals_for_filter.items():
        file = TOTALS_FILE.parent / (
            TOTALS_FILE.name + ".{}-{}.csv".format(filter_.name, lang)
        )
        queries = sorted(next(iter(totals_for_lang.values())))

        totals_for_file: Dict[str, object] = defaultdict(int)
        with file.open("w", encoding="UTF-8") as fout:
            csv_writer = csv.DictWriter(fout, ["date"] + queries + ["TOTAL"])
            csv_writer.writeheader()

            for date_ in sorted(totals_for_lang.keys()):
                totals_for_date = totals_for_lang[date_]
                row: Dict[str, object] = {
                    query: totals_for_date[query] for query in queries
                }
                row["TOTAL"] = sum(row.values())
                for k, v in row.items():
                    assert isinstance(v, int)
                    old = cast(int, totals_for_file[k])
                    totals_for_file[k] = old + v
                row["date"] = date_
                csv_writer.writerow(row)

            totals_for_file["date"] = "TOTAL"
            csv_writer.writerow(totals_for_file)
