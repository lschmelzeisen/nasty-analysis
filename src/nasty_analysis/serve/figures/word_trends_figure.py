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
from datetime import date, timedelta, timezone
from functools import partial
from logging import getLogger
from pathlib import Path
from threading import Thread
from time import time
from typing import (
    Callable,
    Hashable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)

from bokeh.layouts import column, row
from bokeh.models import (
    Button,
    ColumnDataSource,
    CustomJS,
    DatetimeTickFormatter,
    Div,
    NumeralTickFormatter,
)
from bokeh.palettes import Category20_20
from bokeh.plotting import Figure
from elasticsearch_dsl import Search
from nasty_utils import ColoredBraceStyleAdapter, date_range, date_to_datetime
from tornado.gen import coroutine

from nasty_analysis.search_helper import SearchHelper
from nasty_analysis.serve.widgets.dataset_words_widget import DatasetWordsWidget
from nasty_analysis.serve.widgets.date_range_widget import DateRangeWidget
from nasty_analysis.serve.widgets.word_trends_widget import WordTrendsWidget
from nasty_analysis.settings import DatasetType

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WordTrendsFigure:
    def __init__(
        self,
        min_date: date,
        max_date: date,
        date_range_widget: DateRangeWidget,
        dataset_words_widgets: Sequence[DatasetWordsWidget],
        word_trends_widget: WordTrendsWidget,
        add_next_tick_callback: Callable[[Callable[[], None]], None],
    ):
        self._min_date = min_date
        self._max_date = max_date
        self._date_range_widget = date_range_widget
        self._dataset_words_widgets = dataset_words_widgets
        self._word_trends_widget = word_trends_widget
        self._add_next_tick_callback = add_next_tick_callback

        self._source = ColumnDataSource(self._new_source_data())
        self._stats = Div(
            text="",
            style={"padding": "6px 0", "width": "100%"},
            sizing_mode="stretch_width",
        )

        trends_figure = Figure(
            toolbar_location="above",
            tools="save",
            x_axis_type="datetime",
            y_axis_type="linear",
            sizing_mode="stretch_both",
        )
        trends_figure.toolbar.autohide = True
        trends_figure.toolbar.logo = None
        trends_figure.xaxis[0].formatter = DatetimeTickFormatter(
            days=["%d %b"], months=["%b"]
        )
        trends_figure.yaxis[0].formatter = NumeralTickFormatter(format="0a")

        color_palette = Category20_20
        self._glyphs = []
        for i in range(len(color_palette)):
            self._glyphs.append(
                (
                    trends_figure.line(
                        x="dates",
                        # y="dates",
                        source=self._source,
                        color=color_palette[i],
                        line_width=3,
                        visible=False,
                    ),
                    trends_figure.circle(
                        x="dates",
                        # y="dates",
                        source=self._source,
                        color=color_palette[i],
                        size=9,
                        visible=False,
                    ),
                )
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
            trends_figure,
        )

        self._last_selection: Sequence[Optional[Hashable]] = [
            None for _ in range(len(self._dataset_words_widgets))
        ]
        self._word_freqs_per_day: Sequence[Optional[Mapping[str, Sequence[int]]]] = [
            None for _ in range(len(self._dataset_words_widgets))
        ]
        self._num_docs_per_day: Sequence[Optional[Sequence[int]]] = [
            None for _ in range(len(self._dataset_words_widgets))
        ]
        self._took_msecs: Sequence[Optional[int]] = [
            None for _ in range(len(self._dataset_words_widgets))
        ]

    @classmethod
    def _new_source_data(cls) -> MutableMapping[str, List[object]]:
        return {"dates": []}

    def selection(self, index: int) -> Hashable:
        widget = self._dataset_words_widgets[index]
        result = {
            "min_and_max_date": self._date_range_widget.min_and_max_date,
            "dataset": widget.dataset_widget.dataset,
            "lang": widget.dataset_widget.lang,
            "cooccur_words": widget.dataset_widget.cooccur_words,
        }

        if widget.dataset_widget.dataset.type == DatasetType.NASTY:
            result.update(
                {
                    "search_filter": widget.dataset_widget.search_filter,
                    "search_query": widget.dataset_widget.search_query,
                    "user_verified": widget.dataset_widget.user_verified,
                }
            )

        if (
            widget.dataset_widget.dataset.type == DatasetType.NEWS_CSV
            or widget.dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            result.update({"url_netloc": widget.dataset_widget.url_netloc})

        if (
            widget.dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NASTY
            or widget.dataset_widget.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            result.update({"code_identifier": widget.dataset_widget.code_identifier})

        result.update({"words": widget.words})
        return result

    def _compute_update(self) -> None:
        _LOGGER.debug("Computing update.")

        for i in range(len(self._dataset_words_widgets)):
            selection = self.selection(i)
            if self._last_selection[i] != selection:
                (  # Ensure "atomic" update via tuple assignment.
                    self._last_selection[i],
                    (
                        self._word_freqs_per_day[i],
                        self._num_docs_per_day[i],
                        self._took_msecs[i],
                    ),
                ) = (
                    selection,
                    self._fetch_word_freqs_per_day(self._dataset_words_widgets[i]),
                )

        dates = list(date_range(self._min_date, self._max_date))
        min_date, max_date = self._date_range_widget.min_and_max_date
        date_slice = slice(dates.index(min_date), dates.index(max_date) + 1)

        new_data = self._new_source_data()
        new_data["dates"] = [
            date_to_datetime(day, tzinfo_=timezone.utc)
            for day in date_range(min_date, max_date)
        ]
        for i, dataset_word_widget in enumerate(self._dataset_words_widgets):
            if dataset_word_widget.should_num_docs:
                new_data["d{}_num_docs".format(i)] = self._num_docs_per_day[i][
                    date_slice
                ]
            for word in dataset_word_widget.words:
                new_data["d{}_{}".format(i, word)] = self._word_freqs_per_day[i][word][
                    date_slice
                ]

        self._add_next_tick_callback(
            partial(self._display_update, new_data, sum(self._took_msecs))
        )

    @coroutine
    def _display_update(
        self, new_data: Mapping[str, List[object]], took_msecs: int
    ) -> None:
        _LOGGER.debug("Displaying update.")

        self._date_range_widget.set_enabled(True)
        for dataset_words_widget in self._dataset_words_widgets:
            dataset_words_widget.set_enabled(True)
        self._word_trends_widget.set_enabled(True)

        glyphs = iter(self._glyphs)
        try:
            self._source.data = new_data

            for key in new_data:
                if key == "dates":
                    continue

                line, circle = next(glyphs)
                line.glyph.y = key
                line.visible = True
                circle.glyph.y = key
                circle.visible = True

            self._stats.text = f"""
                Request took: <strong>{took_msecs:,}&thinsp;ms</strong>
            """
        except StopIteration:
            self._stats.text = f"""
                <strong style="color: red;">Error: can only visualize
                {len(self._glyphs)} things at once.</strong>
            """

    def update(self) -> None:
        self._date_range_widget.set_enabled(False)
        for dataset_words_widget in self._dataset_words_widgets:
            dataset_words_widget.set_enabled(False)
        self._word_trends_widget.set_enabled(False)

        self._stats.text = """
            <strong style="color: red;">Loading...</strong>
        """
        for line, circle in self._glyphs:
            line.visible = False
            circle.visible = False

        Thread(target=self._compute_update).start()

    def _fetch_word_freqs_per_day(
        self, dataset_words_widget: DatasetWordsWidget
    ) -> Tuple[Mapping[str, Sequence[int]], Sequence[int], int]:
        _LOGGER.debug("Fetching word frequencies per day.")

        search_helper = SearchHelper(dataset_words_widget.dataset_widget.dataset.type)
        search = Search().extra(size=0, track_total_hits=True)
        search = dataset_words_widget.dataset_widget.set_search(search)
        search = search.filter(
            search_helper.query_date_range(
                gte=self._min_date, lt=self._max_date + timedelta(days=1)
            )
        )
        search = search_helper.add_agg_text_tokens_date_histogram_terms(
            search,
            calendar_interval="1d",
            size=len(dataset_words_widget.words),
            include=dataset_words_widget.words,
        )

        time_before = time()
        response = search.execute()
        time_after = time()
        took_msecs = int((time_after - time_before) * 1000)

        dates = list(date_range(self._min_date, self._max_date))
        word_freqs = defaultdict(lambda: [0] * len(dates))
        num_docs = [0] * len(dates)
        for (
            bucket,
            inner_buckets,
        ) in search_helper.read_text_tokens_date_histogram_terms(response):
            i = dates.index(date.fromtimestamp(bucket.key / 1000))
            num_docs[i] = bucket.doc_count
            for inner_bucket in inner_buckets:
                word_freqs[inner_bucket.key][i] = inner_bucket.doc_count

        return word_freqs, num_docs, took_msecs
