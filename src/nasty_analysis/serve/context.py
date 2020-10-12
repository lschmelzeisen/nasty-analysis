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
from logging import getLogger
from time import time
from typing import (
    AbstractSet,
    Callable,
    Counter,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Response
from elasticsearch_dsl.response.aggs import Bucket
from nasty_utils import ColoredBraceStyleAdapter

from nasty_analysis.search_helper import SearchHelper
from nasty_analysis.settings import DatasetSection, DatasetType, NastyAnalysisSettings

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class Context:
    def __init__(self, settings: NastyAnalysisSettings):
        _LOGGER.debug("Initializing new visualization app context.")
        time_before = time()

        datasets = settings.analysis.datasets
        if not datasets:
            raise ValueError("No datasets given.")

        self.settings = settings
        self.min_date, self.max_date = self._fetch_min_and_max_dates(datasets)
        self.lang_freqs_by_dataset = self._fetch_agg_terms_by_dataset(
            datasets,
            SearchHelper.add_agg_lang_terms,
            SearchHelper.read_agg_lang_terms,
            "Languages",
        )
        self.query_freqs_by_dataset = self._fetch_agg_terms_by_dataset(
            datasets,
            SearchHelper.add_agg_nasty_query_terms,
            SearchHelper.read_agg_nasty_query_terms,
            "Queries",
            {DatasetType.NASTY},
        )
        self.url_netloc_freqs_by_dataset = self._fetch_agg_terms_by_dataset(
            datasets,
            SearchHelper.add_agg_news_csv_url_netloc_terms,
            SearchHelper.read_agg_news_csv_url_netloc_terms,
            "Domains",
            {DatasetType.NEWS_CSV, DatasetType.MAXQDA_CODED_NEWS_CSV},
        )

        time_after = time()
        _LOGGER.debug("  Done after {:.2f}s.", time_after - time_before)

    @classmethod
    def _fetch_min_and_max_dates(
        cls, datasets: Sequence[DatasetSection]
    ) -> Tuple[date, date]:
        min_date: Optional[date] = None
        max_date: Optional[date] = None

        for dataset in datasets:
            search_helper = SearchHelper(dataset.type)
            search = Search(index=dataset.index).extra(size=0)
            search = search.filter(
                search_helper.query_date_range(gte=date(2000, 1, 1))
            )  # Needed because missing date defaults to 1900 for some entries.
            search = search_helper.add_agg_date_min_max(search)
            response = search.execute()

            dataset_min_date, dataset_max_date = search_helper.read_agg_date_min_max(
                response
            )

            _LOGGER.debug(
                "Dates of dataset '{}' range from {} to {}.",
                dataset.name,
                dataset_min_date,
                dataset_max_date,
            )

            min_date = min(min_date, dataset_min_date) if min_date else dataset_min_date
            max_date = max(max_date, dataset_max_date) if max_date else dataset_max_date

        if not min_date or not max_date:
            raise ValueError("Could not determine min or max date.")

        _LOGGER.debug("Dates over all datasets range from {} to {}", min_date, max_date)

        return min_date, max_date

    @classmethod
    def _fetch_agg_terms_by_dataset(
        cls,
        datasets: Sequence[DatasetSection],
        add_agg_terms: Callable[[SearchHelper, Search, int], Search],
        read_agg_terms: Callable[[SearchHelper, Response], Iterator[Bucket]],
        log_msg: str,
        dataset_types: Optional[AbstractSet[DatasetType]] = None,
    ) -> Mapping[str, Counter[str]]:
        result = {}

        for dataset in datasets:
            if dataset_types and dataset.type not in dataset_types:
                continue

            search_helper = SearchHelper(dataset.type)
            search = Search(index=dataset.index).extra(size=0)
            search = add_agg_terms(search_helper, search, 100)
            response = search.execute()

            result[dataset.name] = Counter[str](
                {
                    bucket.key: bucket.doc_count
                    for bucket in read_agg_terms(search_helper, response)
                }
            )

            _LOGGER.debug(
                log_msg + " of dataset '{}': {}",
                dataset.name,
                ", ".join(
                    "{} ({:,})".format(lang, freq)
                    for lang, freq in result[dataset.name].most_common()
                ),
            )

        return result
