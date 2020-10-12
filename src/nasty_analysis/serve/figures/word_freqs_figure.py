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

from collections import defaultdict
from datetime import date, timedelta
from functools import lru_cache, partial
from logging import getLogger
from pathlib import Path
from threading import Thread
from time import time
from typing import (
    AbstractSet,
    Callable,
    Counter,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
from unicodedata import category

from bokeh.layouts import column, row
from bokeh.models import Button, ColumnDataSource, CustomJS, DataTable, Div, TableColumn
from elasticsearch_dsl import MultiSearch, Search
from nasty_utils import ColoredBraceStyleAdapter, date_range
from stopwordsiso import stopwords
from tornado.gen import coroutine

from nasty_analysis.search_helper import SearchHelper
from nasty_analysis.serve.figures.num_docs_figure import NumDocsFigure
from nasty_analysis.serve.widgets.dataset_widget import DatasetWidget
from nasty_analysis.serve.widgets.date_range_widget import DateRangeWidget
from nasty_analysis.serve.widgets.word_freqs_widget import WordFilter, WordFreqsWidget
from nasty_analysis.settings import DatasetType

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


@lru_cache(maxsize=None, typed=True)
def _get_stopwords(lang: str) -> AbstractSet[str]:
    result = set(stopwords(lang))

    if lang == "en":
        result.update(
            ("'m", "'re", "'s", "'ve", "n't", "nt", "n’t", "’m", "’re", "’s", "’ve")
        )
        result.difference_update(
            (
                "case",
                "cases",
                "help",
                "home",
                "information",
                "man",
                "million",
                "new",
                "novel",
                "state",
                "states",
                "system",
                "today",
                "uk",
                "work",
                "world",
                "year",
                "years",
            )
        )

    elif lang == "de":
        result.update(
            (
                "bleiben",
                "ca.",
                "echt",
                "eher",
                "eigentlich",
                "fast",
                "fest",
                "genau",
                "halt",
                "klar",
                "ne",
                "paar",
                "sogar",
                "trotz",
                "wahrscheinlich",
            )
        )
        result.difference_update(
            (
                "ernst",
                "jahr",
                "jahre",
                "jahren",
                "mensch",
                "menschen",
                "neuen",
                "tag",
                "tage",
                "uhr",
                "wissen",
                "zeit",
            )
        )

    return result


def _filter_non_letters_unicode(s: str) -> str:
    return "".join(c for c in s if category(c).startswith("L"))


class WordFreqsFigure:
    # TODO: bars indicating number of words

    def __init__(
        self,
        top_n_words: int,
        min_date: date,
        max_date: date,
        date_range_widget: DateRangeWidget,
        dataset_widget: DatasetWidget,
        word_freqs_widget: WordFreqsWidget,
        num_docs_figure: NumDocsFigure,
        add_next_tick_callback: Callable[[Callable[[], None]], None],
    ):
        self._top_n_words = top_n_words
        self._min_date = min_date
        self._max_date = max_date
        self._date_range_widget = date_range_widget
        self._dataset_widget = dataset_widget
        self._word_freqs_widget = word_freqs_widget
        self._num_docs_figure = num_docs_figure
        self._add_next_tick_callback = add_next_tick_callback

        self._source = ColumnDataSource(self._new_source_data())
        self._stats = Div(
            text="",
            style={"padding": "6px 0", "width": "100%"},
            sizing_mode="stretch_width",
        )
        freqs_table = DataTable(
            source=self._source,
            columns=[
                TableColumn(field="words", title="Word"),
                TableColumn(field="freqs", title="Frequency"),
            ],
            sizing_mode="stretch_both",
        )
        export_csv = Button(
            label="Export CSV",
            button_type="primary",
            sizing_mode="fixed",
            width=100,
            height=30,
        )
        export_csv.js_on_click(
            CustomJS(
                args={"source": self._source},
                code=(Path(__file__).parent / "export_csv.js").read_text(
                    encoding="UTF-8"
                ),
            )
        )
        self.figure = column(
            row(self._stats, export_csv, sizing_mode="stretch_width"),
            freqs_table,
        )

        self._last_selection: Optional[Hashable] = None
        self._word_freqs_per_day: Optional[Mapping[str, Sequence[int]]] = None
        self._num_docs_per_day: Optional[Sequence[int]] = None
        self._took_msecs: Optional[int] = None

    @classmethod
    def _new_source_data(cls) -> Mapping[str, List[object]]:
        return {
            "words": [],
            "freqs": [],
        }

    @property
    def selection(self) -> Hashable:
        result = {
            "dataset": self._dataset_widget.dataset,
            "lang": self._dataset_widget.lang,
            "cooccur_words": self._dataset_widget.cooccur_words,
        }

        if self._dataset_widget.dataset.type == DatasetType.NASTY:
            result.update(
                {
                    "search_filter": self._dataset_widget.search_filter,
                    "search_query": self._dataset_widget.search_query,
                    "user_verified": self._dataset_widget.user_verified,
                }
            )

        if (
            self._dataset_widget.dataset.type == DatasetType.NEWS_CSV
            or self._dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            result.update({"url_netloc": self._dataset_widget.url_netloc})

        if (
            self._dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NASTY
            or self._dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            print(self._dataset_widget.code_identifier)
            result.update({"code_identifier": self._dataset_widget.code_identifier})

        return result

    def _compute_update(self) -> None:
        _LOGGER.debug("Computing update.")

        # TODO: add lock around this if?
        selection = self.selection
        if self._last_selection != selection:
            (  # Ensure "atomic" update via tuple assignment.
                self._last_selection,
                (self._word_freqs_per_day, self._num_docs_per_day, self._took_msecs),
            ) = (selection, self._fetch_word_freqs_per_day(self._dataset_widget))
            self._add_next_tick_callback(
                partial(self._num_docs_figure.display_update, self._num_docs_per_day)
            )

        dates = list(date_range(self._min_date, self._max_date))
        min_date, max_date = self._date_range_widget.min_and_max_date
        date_slice = slice(dates.index(min_date), dates.index(max_date) + 1)

        num_docs = sum(self._num_docs_per_day[date_slice])

        word_freqs = Counter[str]()
        if self._word_freqs_widget.should_normalize:
            smoothing_factor = 0.1
            smoothing_denominator = smoothing_factor * len(
                self._num_docs_per_day[date_slice]
            )

            for word, freqs_per_day in self._word_freqs_per_day.items():
                normalized_freq_sum = 0
                for num_docs_on_day, freq_on_day in zip(
                    self._num_docs_per_day[date_slice], freqs_per_day[date_slice]
                ):
                    normalized_freq_sum += (freq_on_day + smoothing_factor) / (
                        num_docs_on_day + smoothing_denominator
                    )
                word_freqs[word] = normalized_freq_sum

        else:
            for word, freqs_per_day in self._word_freqs_per_day.items():
                word_freqs[word] = sum(freqs_per_day[date_slice])

        word_filter = self._word_freqs_widget.word_filter
        stopwords = set()
        if word_filter == WordFilter.ONLY_NON_STOPWORDS:
            stopwords = _get_stopwords(self._dataset_widget.lang)

        new_data = self._new_source_data()
        for word, freq in word_freqs.most_common(self._top_n_words):
            if not freq:
                break
            if (
                (
                    word_filter == WordFilter.ONLY_NON_STOPWORDS
                    and (word in stopwords or not _filter_non_letters_unicode(word))
                )
                or (
                    word_filter == WordFilter.ONLY_HASHTAGS and not word.startswith("#")
                )
                or (
                    word_filter == WordFilter.ONLY_MENTIONS and not word.startswith("@")
                )
            ):
                continue

            new_data["words"].append(word)
            new_data["freqs"].append(freq)

        self._add_next_tick_callback(
            partial(self._display_update, new_data, num_docs, self._took_msecs)
        )

    @coroutine
    def _display_update(
        self, new_data: Mapping[str, List[object]], num_docs: int, took_msecs: int
    ) -> None:
        _LOGGER.debug("Displaying update.")

        self._date_range_widget.set_enabled(True)
        self._dataset_widget.set_enabled(True)
        self._word_freqs_widget.set_enabled(True)

        self._source.data = new_data
        self._stats.text = f"""
            # matching documents: <strong>{num_docs:,}</strong>
            &nbsp;&centerdot;&nbsp;
            Request took: <strong>{took_msecs:,}&thinsp;ms</strong>
        """

    def update(self) -> None:
        self._date_range_widget.set_enabled(False)
        self._dataset_widget.set_enabled(False)
        self._word_freqs_widget.set_enabled(False)

        self._source.data = self._new_source_data()
        self._stats.text = """
            <strong style="color: red;">Loading...</strong>
        """
        if self._last_selection != self.selection:
            self._num_docs_figure.display_update([])

        Thread(target=self._compute_update).start()

    def _fetch_word_freqs_per_day(
        self,
        dataset_widget: DatasetWidget,
    ) -> Tuple[Mapping[str, Sequence[int]], Sequence[int], int]:
        _LOGGER.debug("Fetching word frequencies per day.")

        search_helper = SearchHelper(dataset_widget.dataset.type)
        search_template = Search().extra(size=0, track_total_hits=True)
        search_template = dataset_widget.set_search(search_template)
        search_template = search_helper.add_agg_text_tokens_terms(
            search_template, size=self._top_n_words
        )

        search = MultiSearch()
        for cur_date in date_range(self._min_date, self._max_date):
            search = search.add(
                search_template.filter(
                    search_helper.query_date_range(
                        gte=cur_date, lt=cur_date + timedelta(days=1)
                    )
                )
            )

        time_before = time()
        responses = search.execute()
        time_after = time()
        took_msecs = int((time_after - time_before) * 1000)

        word_freqs = defaultdict(lambda: [0] * len(responses))
        num_docs = []
        for i, response in enumerate(responses):
            num_docs.append(response.hits.total.value)
            for bucket in search_helper.read_agg_text_tokens_terms(response):
                word_freqs[bucket.key][i] = bucket.doc_count

        return word_freqs, num_docs, took_msecs
