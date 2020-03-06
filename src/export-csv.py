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
from operator import attrgetter
from pathlib import Path
from shutil import rmtree

import nasty  # type: ignore
from tqdm import tqdm

crawl_dir = Path("twitter-crawl")
csv_dir = crawl_dir.parent / (crawl_dir.name + ".csv")
if csv_dir.exists():
    rmtree(csv_dir)

twitter_crawl = nasty.BatchResults(crawl_dir)
for entry in tqdm(twitter_crawl, desc="Twitter-Crawl-Entries"):
    csv_fields = [
        "url",
        "created_at",
        "reply_count",
        "retweet_count",
        "favorite_count",
        "full_text",
    ]
    tweets = sorted(twitter_crawl.tweets(entry), key=attrgetter("created_at"))

    file = (
        csv_dir
        / entry.request.filter.name.lower()
        / entry.request.lang
        / "{}-{}.csv".format(entry.request.query.replace(" ", "-"), entry.request.since)
    )
    Path.mkdir(file.parent, exist_ok=True, parents=True)

    with open(file, "w", encoding="UTF-8", newline="") as fin:
        csv_writer = csv.DictWriter(fin, csv_fields)
        csv_writer.writeheader()

        for tweet in tweets:
            tweet_json = tweet.to_json()
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
                "user_description": tweet_json["user"]["description"],
                "user_followers_count": tweet_json["user"]["followers_count"],
                "user_friends_count": tweet_json["user"]["friends_count"],
                "user_verified": tweet_json["user"]["verified"],
                "user_statuses_count": tweet_json["user"]["statuses_count"],
            }
            csv_writer.writerow({field: tweet_csv[field] for field in csv_fields})
