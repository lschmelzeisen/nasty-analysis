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
from datetime import date
from logging import getLogger
from pathlib import Path
from time import time
from typing import Mapping, Optional, Sequence

from elasticsearch_dsl import Index, Keyword
from nasty import Batch, BatchResults, Request, Search, SearchFilter
from nasty_data import (
    BaseDocument,
    add_documents_to_index,
    load_document_dicts_from_nasty_batch_results,
)
from nasty_data.elasticsearch_.index import new_index
from nasty_utils import ColoredBraceStyleAdapter
from tqdm import tqdm
from typing_extensions import Final

from nasty_analysis.document.maxqda_coded_nasty import (
    load_document_dicts_from_maxqda_coded_nasty_csv,
)
from nasty_analysis.document.maxqda_coded_news_csv import (
    load_document_dicts_from_maxqda_coded_news_csv,
)
from nasty_analysis.document.news_csv import load_document_dicts_from_news_csv
from nasty_analysis.document.tokenize import (
    TokenizedMaxqdaCodedNastyDocument,
    TokenizedMaxqdaCodedNewsCsvDocument,
    TokenizedNastyBatchResultsTwitterDocument,
    TokenizedNewsCsvDocument,
)
from nasty_analysis.search_helper import SearchHelper
from nasty_analysis.settings import (
    DatasetSection,
    DatasetSourceMaxqdaCodeSection,
    DatasetType,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_INDEXED_SUFFIX: Final[str] = "-indexed"


def _update_nasty_batch_file(
    *,
    batch_file: Path,
    queries: Sequence[str],
    start_date: date,
    end_date: date,
    languages: Sequence[str],
    filters: Sequence[SearchFilter],
    max_tweets: Optional[int],
    batch_size: int,
) -> None:
    def is_request_in_batch(request: Request, batch: Batch) -> bool:
        for entry in batch:
            if request == entry.request:
                return True
        return False

    batch = Batch()
    if batch_file.exists():
        batch.load(batch_file)

    for language in languages:
        for filter_ in filters:
            for query in queries:
                request = Search(
                    query,
                    since=start_date,
                    until=end_date,
                    filter_=filter_,
                    lang=language,
                    max_tweets=max_tweets,
                    batch_size=batch_size,
                )

                for daily_request in request.to_daily_requests():
                    if is_request_in_batch(daily_request, batch):
                        continue
                    batch.append(daily_request)

    batch_file.parent.mkdir(parents=True, exist_ok=True)
    batch.dump(batch_file)


class IndexedFilesDocument(BaseDocument):
    file_name = Keyword(doc_values=False)


class Dataset:
    def __init__(self, settings: DatasetSection, *, max_retries: int, num_procs: int):
        self._settings = settings
        self._max_retries = max_retries
        self._num_procs = num_procs

        if self._settings.type == DatasetType.NASTY:
            if self._settings.source_nasty is None:
                raise ValueError(
                    f"Setting [[analysis.datasets]].type = {DatasetType.NASTY.name} "
                    "requires you to also set .source_nasty."
                )

        elif self._settings.type == DatasetType.NEWS_CSV:
            if self._settings.source_news_csv is None:
                raise ValueError(
                    f"Setting [[analysis.datasets]].type = {DatasetType.NEWS_CSV.name} "
                    "requires you to also set .source_news_csv."
                )

        elif self._settings.type == DatasetType.MAXQDA_CODED_NASTY:
            if self._settings.source_maxqda_coded_nasty is None:
                raise ValueError(
                    "Setting [[analysis.datasets]].type = "
                    f"{DatasetType.MAXQDA_CODED_NASTY.name} requires you to also set "
                    ".source_maxqda_coded_nasty."
                )

        elif self._settings.type == DatasetType.MAXQDA_CODED_NEWS_CSV:
            if self._settings.source_maxqda_coded_news_csv is None:
                raise ValueError(
                    "Setting [[analysis.datasets]].type = "
                    f"{DatasetType.MAXQDA_CODED_NEWS_CSV.name} requires you to also set "
                    ".source_maxqda_coded_news_csv."
                )

        else:
            raise NotImplementedError()

    def retrieve(self) -> None:
        if self._settings.type == DatasetType.NASTY:
            source_nasty = self._settings.source_nasty
            assert source_nasty

            _update_nasty_batch_file(
                batch_file=source_nasty.batch_file,
                queries=source_nasty.queries,
                start_date=source_nasty.start_date,
                end_date=source_nasty.end_date,
                languages=source_nasty.languages,
                filters=source_nasty.filters,
                max_tweets=source_nasty.max_tweets,
                batch_size=source_nasty.batch_size,
            )

            batch = Batch()
            batch.load(source_nasty.batch_file)
            batch.execute(source_nasty.batch_results_dir)

        else:
            raise NotImplementedError(
                f"Can not dataset of type '{self._settings.type}' automatically."
            )

    def index(self) -> None:
        if self._settings.type == DatasetType.NASTY:
            self._index_nasty_dataset()

        elif self._settings.type == DatasetType.NEWS_CSV:
            self._index_news_csv_dataset()

        elif self._settings.type == DatasetType.MAXQDA_CODED_NASTY:
            self._index_maxqda_coded_nasty_dataset()

        elif self._settings.type == DatasetType.MAXQDA_CODED_NEWS_CSV:
            self._index_maxqda_coded_news_csv_dataset()

        else:
            raise NotImplementedError()

    def _index_nasty_dataset(self) -> None:
        source = self._settings.source_nasty
        assert source

        if not Index(self._settings.index).exists():
            new_index(self._settings.index, TokenizedNastyBatchResultsTwitterDocument)

        indexed_index = self._settings.index + _INDEXED_SUFFIX
        if not Index(indexed_index).exists():
            new_index(indexed_index, IndexedFilesDocument)

        batch_results = BatchResults(source.batch_results_dir)
        total_size = 0
        for batch_entry in batch_results:
            data_file = source.batch_results_dir / batch_entry.data_file_name
            total_size += data_file.stat().st_size

        with tqdm(
            desc=self._settings.name,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
            position=1,
        ) as progress_bar:
            for batch_entry in batch_results:
                data_file = source.batch_results_dir / batch_entry.data_file_name

                if (
                    IndexedFilesDocument.search(index=indexed_index)
                    .query("term", file_name=data_file.name)
                    .execute()
                ):
                    _LOGGER.debug(
                        "Data file '{}' is already indexed, skipping.",
                        data_file.name,
                    )
                    progress_bar.update(data_file.stat().st_size)
                    continue

                add_documents_to_index(
                    self._settings.index,
                    TokenizedNastyBatchResultsTwitterDocument,
                    load_document_dicts_from_nasty_batch_results(data_file),
                    max_retries=self._max_retries,
                    num_procs=self._num_procs,
                )
                IndexedFilesDocument(file_name=data_file.name).save(index=indexed_index)
                progress_bar.update(data_file.stat().st_size)

    def _index_news_csv_dataset(self) -> None:
        source = self._settings.source_news_csv
        assert source

        new_index(self._settings.index, TokenizedNewsCsvDocument)
        add_documents_to_index(
            self._settings.index,
            TokenizedNewsCsvDocument,
            load_document_dicts_from_news_csv(source.file, lang=source.lang),
            max_retries=self._max_retries,
            num_procs=self._num_procs,
        )

    def _index_maxqda_coded_nasty_dataset(self) -> None:
        source = self._settings.source_maxqda_coded_nasty
        assert source

        new_index(self._settings.index, TokenizedMaxqdaCodedNastyDocument)
        self._index_maxqda_coded_nasty_code(source.codes, source.lang)

    def _index_maxqda_coded_nasty_code(
        self, codes: Sequence[DatasetSourceMaxqdaCodeSection], lang: str
    ) -> None:
        for code in codes:
            if code.file:
                add_documents_to_index(
                    self._settings.index,
                    TokenizedMaxqdaCodedNastyDocument,
                    load_document_dicts_from_maxqda_coded_nasty_csv(
                        code.file, code.code_identifier, lang
                    ),
                    max_retries=self._max_retries,
                    num_procs=self._num_procs,
                )

            if code.codes:
                self._index_maxqda_coded_nasty_code(code.codes, lang)

    def _index_maxqda_coded_news_csv_dataset(self) -> None:
        source = self._settings.source_maxqda_coded_news_csv
        assert source

        news_csv_document_dicts = {}
        for document_dict in load_document_dicts_from_news_csv(
            source.file, lang=source.lang, progress_bar=False
        ):
            news_csv_document_dicts[str(document_dict["index"])] = document_dict

        new_index(self._settings.index, TokenizedMaxqdaCodedNewsCsvDocument)
        add_documents_to_index(
            self._settings.index,
            TokenizedMaxqdaCodedNewsCsvDocument,
            tqdm(
                news_csv_document_dicts.values(),
                desc=source.file.name,
                total=len(news_csv_document_dicts),
                dynamic_ncols=True,
            ),
            max_retries=self._max_retries,
            num_procs=self._num_procs,
        )
        self._index_maxqda_coded_news_csv_code(source.codes, news_csv_document_dicts)

    def _index_maxqda_coded_news_csv_code(
        self,
        codes: Sequence[DatasetSourceMaxqdaCodeSection],
        news_csv_document_dicts: Mapping[str, Mapping[str, object]],
    ) -> None:
        for code in codes:
            if code.file:
                add_documents_to_index(
                    self._settings.index,
                    TokenizedMaxqdaCodedNewsCsvDocument,
                    load_document_dicts_from_maxqda_coded_news_csv(
                        code.file,
                        code.code_identifier,
                        news_csv_document_dicts,
                    ),
                    max_retries=self._max_retries,
                    num_procs=self._num_procs,
                )

            if code.codes:
                self._index_maxqda_coded_news_csv_code(
                    code.codes, news_csv_document_dicts
                )

    def export(self, query_string: str, output_file: Path) -> None:
        search_helper = SearchHelper(self._settings.type)
        search = (
            Index(self._settings.index)
            .search()
            .extra(size=0, track_total_hits=True)
            .query(search_helper.query_text_query_string(query_string))
        )

        time_before = time()
        response = search.execute()
        time_after = time()
        _LOGGER.debug("Search took {:.2}s", time_after - time_before)

        if self._settings.type == DatasetType.NASTY:
            fieldnames = (
                "created_at",
                "favorite_count",
                "full_text",
                "full_text_orig",
                "id_str",
                "lang",
                "place.country",
                "place.country_code",
                "place.full_name",
                "place.id",
                "place.name",
                "place.place_type",
                "place.url",
                "quoted_status_id",
                "reply_count",
                "retweet_count",
                "user.description",
                "user.description_orig",
                "user.favourites_count",
                "user.followers_count",
                "user.friends_count",
                "user.geo_enabled",
                "user.id_str",
                "user.location",
                "user.name",
                "user.screen_name",
                "user.statuses_count",
                "user.verified",
            )

        elif (
            self._settings.type == DatasetType.NEWS_CSV
            or self._settings.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            fieldnames = (
                "_id",
                "lang",
                "text",
                "text_orig",
                "time",
                "title",
                "title_orig",
                "url",
                "url_netloc",
            )

            if self._settings.type == DatasetType.MAXQDA_CODED_NEWS_CSV:
                fieldnames = fieldnames + (
                    "document_id",
                    "document_group",
                    "code_identifier" "code",
                    "segment",
                    "coverage",
                )

        elif self._settings.type == DatasetType.MAXQDA_CODED_NASTY:
            fieldnames = (
                "_id",
                "document_group",
                "code_identifier",
                "lang",
                "created_at",
                "code",
                "segment",
                "coverage",
            )

        else:
            raise NotImplementedError()

        num_expected_documents = response.hits.total.value
        num_received_documents = 0

        with output_file.open("w", encoding="UTF-8", newline="") as fout:
            csv_writer = csv.DictWriter(
                fout, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC
            )
            csv_writer.writeheader()

            for document in tqdm(
                search.scan(),
                desc=output_file.name,
                total=num_expected_documents,
                dynamic_ncols=True,
            ):
                num_received_documents += 1

                csv_row = {}
                for fieldname in fieldnames:
                    if fieldname == "_id":
                        csv_row[fieldname] = document.meta.id
                        continue

                    value = document
                    for segment in fieldname.split("."):
                        if segment not in value:
                            value = None
                            break
                        else:
                            value = getattr(value, segment)
                    csv_row[fieldname] = value

                csv_writer.writerow(csv_row)

        if num_expected_documents != num_received_documents:
            _LOGGER.warning(
                "Expected {} documents, but received {}.",
                num_expected_documents,
                num_expected_documents,
            )
