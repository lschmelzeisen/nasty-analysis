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
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Counter

from nasty import BatchEntry, BatchResults, Search
from somajo import SoMaJo
from tqdm import tqdm

from src._util.io_ import _write_file_with_tmp_guard
from src.config import CRAWL_DIR, NUM_PROCESSORS, frequencies_file

tokenizer = {
    "en": SoMaJo("en_PTB", split_sentences=False),
    "de": SoMaJo("de_CMC", split_sentences=False),
}

twitter_crawl = BatchResults(CRAWL_DIR)


def calc_freqs_for_batch_entry(entry: BatchEntry) -> None:
    assert isinstance(entry.request, Search)
    assert entry.request.since is not None
    file = frequencies_file(
        entry.request.filter,
        entry.request.lang,
        entry.request.query,
        entry.request.since,
    )
    if file.exists():
        return

    counter = Counter[str]()
    for tweet in twitter_crawl.tweets(entry):
        counter.update(
            token.text.lower()
            for token in next(tokenizer[entry.request.lang].tokenize_text([tweet.text]))
            if (
                token.token_class
                in ["regular", "hashtag", "number_compound", "abbreviation"]
            )
        )

    Path.mkdir(file.parent, exist_ok=True, parents=True)
    with _write_file_with_tmp_guard(file, newline="") as fout:
        csv_writer = csv.writer(fout)
        for word, freq in counter.most_common():
            csv_writer.writerow([word, freq])


def main() -> None:
    with ProcessPoolExecutor(NUM_PROCESSORS) as executor:
        results = {
            executor.submit(calc_freqs_for_batch_entry, entry)
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
