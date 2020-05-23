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
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Mapping, cast

from nasty import BatchEntry, BatchResults, Search
from tqdm import tqdm

from nasty_analysis._util.io_ import _write_file_with_tmp_guard
from nasty_analysis.config import CRAWL_DIR, CSV_FIELDS, NUM_PROCESSORS, csv_file

twitter_crawl = BatchResults(CRAWL_DIR)


def export_csv_for_batch_entry(entry: BatchEntry) -> None:
    assert isinstance(entry.request, Search)
    assert entry.request.since is not None
    file = csv_file(
        entry.request.filter,
        entry.request.lang,
        entry.request.query,
        entry.request.since,
    )
    if file.exists():
        return

    Path.mkdir(file.parent, exist_ok=True, parents=True)
    with _write_file_with_tmp_guard(file, newline="") as fout:
        csv_writer = csv.DictWriter(fout, CSV_FIELDS)
        csv_writer.writeheader()

        for tweet in twitter_crawl.tweets(entry):
            tweet_json = tweet.to_json()
            user_json = cast(Mapping[str, object], tweet_json["user"])
            tweet_csv = {
                "url": tweet.url,
                "created_at": tweet.created_at,
                "full_text": tweet.text.replace("\n", " "),
                "is_answer_to_quote": "quoted_status_id_str" in tweet_json,
                "is_reply_to_status": "in_reply_to_status_id_str" in tweet_json,
                "retweet_count": tweet_json["retweet_count"],
                "favorite_count": tweet_json["favorite_count"],
                "reply_count": tweet_json["reply_count"],
                "user_name": tweet.user.name,
                "user_screen_name": tweet.user.screen_name,
                "user_description": user_json["description"],
                "user_followers_count": user_json["followers_count"],
                "user_friends_count": user_json["friends_count"],
                "user_verified": user_json["verified"],
                "user_statuses_count": user_json["statuses_count"],
            }
            csv_writer.writerow({field: tweet_csv[field] for field in CSV_FIELDS})


def main() -> None:
    with ProcessPoolExecutor(NUM_PROCESSORS) as executor:
        results = {
            executor.submit(export_csv_for_batch_entry, entry)
            for entry in twitter_crawl
        }
        for _result in tqdm(
            as_completed(results),
            total=len(twitter_crawl),
            desc="Twitter-Crawl Entries",
        ):
            pass


if __name__ == "__main__":
    main()
