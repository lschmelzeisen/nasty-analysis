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

from copy import copy
from datetime import date
from typing import Iterator, Optional, Sequence, Tuple

from elasticsearch_dsl import AttrDict, Search, aggs, query
from elasticsearch_dsl.query import Query
from elasticsearch_dsl.response import Response
from elasticsearch_dsl.response.aggs import Bucket

from nasty_analysis.settings import DatasetType


class IncorrectDatasetTypeError(Exception):
    pass


class SearchHelper:
    def __init__(self, dataset_type: DatasetType):
        self._dataset_type = dataset_type

    @property
    def _date_field(self) -> str:
        return {
            DatasetType.NASTY: "created_at",
            DatasetType.NEWS_CSV: "time",
            DatasetType.MAXQDA_CODED_NASTY: "created_at",
            DatasetType.MAXQDA_CODED_NEWS_CSV: "time",
        }[self._dataset_type]

    def query_date_range(
        self,
        gt: Optional[date] = None,
        gte: Optional[date] = None,
        lt: Optional[date] = None,
        lte: Optional[date] = None,
    ) -> Query:
        if gt and gte:
            raise ValueError("Can not use gt together with gte.")
        if lt and lte:
            raise ValueError("Can not use lt together with lte.")

        range_kwargs = {}
        if gt:
            range_kwargs["gt"] = gt
        if gte:
            range_kwargs["gte"] = gte
        if lt:
            range_kwargs["lt"] = lt
        if lte:
            range_kwargs["lte"] = lte
        if not range_kwargs:
            raise ValueError("At least one of gt, gte, lt, lte must be given.")

        return query.Range(**{self._date_field: range_kwargs})

    def add_agg_date_min_max(self, search: Search) -> Search:
        search = copy(search)
        search.aggs.metric("min_date", aggs.Min(field=self._date_field))
        search.aggs.metric("max_date", aggs.Max(field=self._date_field))
        return search

    def read_agg_date_min_max(self, response: Response) -> Tuple[date, date]:
        return (
            date.fromtimestamp(response.aggs.min_date.value / 1000),
            date.fromtimestamp(response.aggs.max_date.value / 1000),
        )

    @classmethod
    def _query_term(cls, value: object, field: Sequence[str]) -> Query:
        q = query.Term(**{".".join(field): value})
        for i in range(len(field) - 2, -1, -1):
            q = query.Nested(path=".".join(field[: i + 1]), query=q)
        return q

    @classmethod
    def _add_agg_terms(cls, search: Search, size: int, field: Sequence[str]) -> Search:
        search = copy(search)
        a = search.aggs
        for i in range(len(field) - 1):
            a = a.bucket(
                field[i].replace(".", "__"), aggs.Nested(path=".".join(field[: i + 1]))
            )
        a.bucket(
            field[-1].replace(".", "__"), aggs.Terms(field=".".join(field), size=size)
        )
        return search

    @classmethod
    def _read_agg_terms(
        cls, response: Response, field: Sequence[str]
    ) -> Iterator[Bucket]:
        result = response.aggs
        for f in field:
            result = getattr(result, f.replace(".", "__"))
        yield from result.buckets

    @classmethod
    def _query_terms(cls, values: Sequence[object], field: Sequence[str]) -> Query:
        q = query.Terms(**{".".join(field): values})
        for i in range(len(field) - 2, -1, -1):
            q = query.Nested(path=".".join(field[: i + 1]), query=q)
        return q

    @property
    def _lang_field(self) -> Sequence[str]:
        return {
            DatasetType.NASTY: ("nasty_batch_meta", "request.lang"),
            DatasetType.NEWS_CSV: ("lang",),
            DatasetType.MAXQDA_CODED_NASTY: ("lang",),
            DatasetType.MAXQDA_CODED_NEWS_CSV: ("lang",),
        }[self._dataset_type]

    def query_lang_term(self, value: str) -> Query:
        return self._query_term(value, self._lang_field)

    def add_agg_lang_terms(self, search: Search, size: int) -> Search:
        return self._add_agg_terms(search, size, self._lang_field)

    def read_agg_lang_terms(self, response: Response) -> Iterator[Bucket]:
        return self._read_agg_terms(response, self._lang_field)

    @property
    def _text_field(self) -> str:
        return {
            DatasetType.NASTY: "full_text",
            DatasetType.NEWS_CSV: "text",
            DatasetType.MAXQDA_CODED_NASTY: "segment",
            DatasetType.MAXQDA_CODED_NEWS_CSV: "segment",
        }[self._dataset_type]

    def query_text_query_string(self, query_string: str) -> Query:
        return query.QueryString(query=query_string, default_field=self._text_field)

    @property
    def _text_tokens_field(self) -> str:
        return {
            DatasetType.NASTY: "full_text_tokens",
            DatasetType.NEWS_CSV: "text_tokens",
            DatasetType.MAXQDA_CODED_NASTY: "segment_tokens",
            DatasetType.MAXQDA_CODED_NEWS_CSV: "segment_tokens",
        }[self._dataset_type]

    def query_text_tokens_term(self, value: str) -> Query:
        return self._query_term(value, (self._text_tokens_field,))

    def add_agg_text_tokens_terms(self, search: Search, size: int) -> Search:
        return self._add_agg_terms(search, size, (self._text_tokens_field,))

    def read_agg_text_tokens_terms(self, response: Response) -> Iterator[Bucket]:
        return self._read_agg_terms(response, (self._text_tokens_field,))

    def add_agg_text_tokens_date_histogram_terms(
        self,
        search: Search,
        calendar_interval: str,
        size: int,
        include: object,
    ) -> Search:
        search = copy(search)
        a = search.aggs.bucket(
            self._date_field.replace(".", "__"),
            aggs.DateHistogram(
                field=self._date_field, calendar_interval=calendar_interval
            ),
        )
        if size:
            a.bucket(
                self._text_tokens_field.replace(".", "__"),
                aggs.Terms(
                    field=self._text_tokens_field,
                    size=size,
                    include=include,
                ),
            )
        return search

    def read_text_tokens_date_histogram_terms(
        self, response: Response
    ) -> Iterator[Tuple[Bucket, Sequence[Bucket]]]:
        for bucket in getattr(
            response.aggs, self._date_field.replace(".", "__")
        ).buckets:
            yield bucket, getattr(
                bucket,
                self._text_tokens_field.replace(".", "__"),
                AttrDict({"buckets": []}),
            ).buckets

    @property
    def _nasty_filter_field(self) -> Sequence[str]:
        return {DatasetType.NASTY: ("nasty_batch_meta", "request.filter")}[
            self._dataset_type
        ]

    def query_nasty_filter_term(self, value: str) -> Query:
        return self._query_term(value, self._nasty_filter_field)

    @property
    def _nasty_query_field(self) -> Sequence[str]:
        return {DatasetType.NASTY: ("nasty_batch_meta", "request.query")}[
            self._dataset_type
        ]

    def query_nasty_query_term(self, value: str) -> Query:
        return self._query_term(value, self._nasty_query_field)

    def add_agg_nasty_query_terms(self, search: Search, size: int) -> Search:
        return self._add_agg_terms(search, size, self._nasty_query_field)

    def read_agg_nasty_query_terms(self, response: Response) -> Iterator[Bucket]:
        return self._read_agg_terms(response, self._nasty_query_field)

    @property
    def _nasty_user_verified_field(self) -> Sequence[str]:
        return {DatasetType.NASTY: ("user.verified",)}[self._dataset_type]

    def query_nasty_user_verified_term(self, value: bool) -> Query:
        return self._query_term(value, self._nasty_user_verified_field)

    @property
    def _news_csv_url_netloc_field(self) -> Sequence[str]:
        return {
            DatasetType.NEWS_CSV: ("url_netloc",),
            DatasetType.MAXQDA_CODED_NEWS_CSV: ("url_netloc",),
        }[self._dataset_type]

    def query_news_csv_url_netloc_term(self, value: str) -> Query:
        return self._query_term(value, self._news_csv_url_netloc_field)

    def add_agg_news_csv_url_netloc_terms(self, search: Search, size: int) -> Search:
        return self._add_agg_terms(search, size, self._news_csv_url_netloc_field)

    def read_agg_news_csv_url_netloc_terms(
        self, response: Response
    ) -> Iterator[Bucket]:
        return self._read_agg_terms(response, self._news_csv_url_netloc_field)

    @property
    def _maxqda_coded_code_identifier_field(self) -> Sequence[str]:
        return {
            DatasetType.MAXQDA_CODED_NASTY: ("code_identifier",),
            DatasetType.MAXQDA_CODED_NEWS_CSV: ("code_identifier",),
        }[self._dataset_type]

    def query_maxqda_coded_code_identifier_terms(self, values: Sequence[str]) -> Query:
        return self._query_terms(values, self._maxqda_coded_code_identifier_field)
