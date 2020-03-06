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

import json
from operator import attrgetter
from pathlib import Path
from shutil import rmtree
from typing import Counter

import nasty  # type: ignore
from nltk.tokenize import TweetTokenizer
from tqdm import tqdm

crawl_dir = Path("twitter-crawl")
freqs_dir = crawl_dir.parent / (crawl_dir.name + ".freqs")
if freqs_dir.exists():
    rmtree(freqs_dir)

tokenizer = TweetTokenizer()

twitter_crawl = nasty.BatchResults(crawl_dir)
for entry in tqdm(twitter_crawl[7:], desc="Twitter-Crawl-Entries"):
    tweets = sorted(twitter_crawl.tweets(entry), key=attrgetter("created_at"))

    counter = Counter[str]()
    for tweet in tweets:
        counter.update(token.lower() for token in tokenizer.tokenize(tweet.text))

    file = (
        freqs_dir
        / entry.request.filter.name.lower()
        / entry.request.lang
        / "{}-{}.freqs".format(
            entry.request.query.replace(" ", "-"), entry.request.since
        )
    )

    Path.mkdir(file.parent, exist_ok=True, parents=True)
    with open(file, "w", encoding="UTF-8", newline="") as fin:
        json.dump(dict(counter.most_common()), fin, indent=0)
