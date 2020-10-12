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

from datetime import date
from enum import Enum
from pathlib import Path
from typing import Optional, Sequence

from nasty import DEFAULT_BATCH_SIZE, DEFAULT_MAX_TWEETS, SearchFilter
from nasty_data import ElasticsearchSettings
from nasty_utils import Settings, SettingsConfig
from pydantic import validator


class DatasetType(Enum):
    NASTY = "NASTY"
    NEWS_CSV = "NEWS_CSV"
    MAXQDA_CODED_NASTY = "MAXQDA_CODED_NASTY"
    MAXQDA_CODED_NEWS_CSV = "MAXQDA_CODED_NEWS_CSV"


class DatasetSourceNastySection(Settings):
    batch_file: Path
    batch_results_dir: Path
    queries: Sequence[str]
    start_date: date
    end_date: date
    languages: Sequence[str]
    filters: Sequence[SearchFilter]
    max_tweets: Optional[int] = DEFAULT_MAX_TWEETS
    batch_size: int = DEFAULT_BATCH_SIZE

    @validator("max_tweets")
    def _max_tweets_validator(cls, value: int) -> Optional[int]:  # noqa: N805
        return value if value >= 0 else None


class DatasetSourceNewsCsvSection(Settings):
    file: Path
    lang: str


class DatasetSourceMaxqdaCodeSection(Settings):
    code_identifier: str
    file: Optional[Path]
    codes: Optional[Sequence["DatasetSourceMaxqdaCodeSection"]]


DatasetSourceMaxqdaCodeSection.update_forward_refs()


def _maxqda_codes_validator(
    value: Sequence[DatasetSourceMaxqdaCodeSection],
) -> Sequence[DatasetSourceMaxqdaCodeSection]:
    codes_stack = list(value)
    code_identifiers = set()

    while codes_stack:
        v = codes_stack.pop()
        if v.code_identifier in code_identifiers:
            raise ValueError(
                "MAXQDA code identifier '{}' used multiple times, but must be "
                "unique.".format(v.code_identifier)
            )
        code_identifiers.add(v.code_identifier)

        codes_stack.extend(v.codes or [])

    return value


class DatasetSourceMaxqdaCodedNastySection(Settings):
    lang: str

    codes: Sequence[DatasetSourceMaxqdaCodeSection]
    _codes_validator = validator("codes", allow_reuse=True)(_maxqda_codes_validator)


class DatasetSourceMaxqdaCodedNewsCsvSection(DatasetSourceNewsCsvSection):
    codes: Sequence[DatasetSourceMaxqdaCodeSection]
    _codes_validator = validator("codes", allow_reuse=True)(_maxqda_codes_validator)


class DatasetSection(Settings):
    name: str
    index: str
    type: DatasetType
    source_nasty: Optional[DatasetSourceNastySection]
    source_news_csv: Optional[DatasetSourceNewsCsvSection]
    source_maxqda_coded_nasty: Optional[DatasetSourceMaxqdaCodedNastySection]
    source_maxqda_coded_news_csv: Optional[DatasetSourceMaxqdaCodedNewsCsvSection]


class WordFreqsSection(Settings):
    enabled: bool = True
    top_n_words: int = 1000


class WordTrendsSection(Settings):
    enabled: bool = True
    num_dataset_word_widgets: int = 4


class _ServeSection(Settings):
    address: str = "localhost"
    port: int = 5006
    word_freqs: WordFreqsSection
    word_trends: WordTrendsSection


class _AnalysisSection(Settings):
    num_procs: int = 2
    datasets: Sequence[DatasetSection]
    serve: _ServeSection


class NastyAnalysisSettings(ElasticsearchSettings):
    class Config(SettingsConfig):
        search_path = Path("nasty.toml")

    analysis: _AnalysisSection
