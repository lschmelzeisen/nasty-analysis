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

from datetime import timedelta
from typing import Counter, List, Mapping

from bokeh.layouts import column, row
from bokeh.models import (
    CheckboxGroup,
    ColumnDataSource,
    DataTable,
    DateRangeSlider,
    Div,
    Panel,
    Select,
    TableColumn,
)
from nasty import SearchFilter
from stopwordsiso import stopwords

from src.config import (
    DAY_RESOLUTION,
    END_DATE,
    FILTERS,
    LANGUAGES,
    QUERIES,
    START_DATE,
    TIME_SPAN,
    TOP_K_MOST_FREQUENT_WORDS,
)
from src.visualize.frequencies import frequencies

STOPWORDS = {language: stopwords(language) for language in LANGUAGES}


class PanelWordFrequencies:
    def __init__(self) -> None:
        self._source = ColumnDataSource({"words": [], "frequencies": []})

        description = Div(
            text="""
                <h1>Word Frequencies</h1>
            """,
            sizing_mode="fixed",
            width=350,
        )

        self._query_select = Select(
            title="Query:",
            options=QUERIES,
            value=QUERIES[0],
            sizing_mode="fixed",
            width=350,
            height=50,
        )
        self._query_select.on_change("value", self.on_change)

        self._language_select = Select(
            title="Language:",
            options=LANGUAGES,
            value=LANGUAGES[0],
            sizing_mode="fixed",
            width=350,
            height=50,
        )
        self._language_select.on_change("value", self.on_change)

        self._filter_select = Select(
            title="Search Filter:",
            options=[filter_.name.lower() for filter_ in FILTERS],
            value=FILTERS[0].name.lower(),
            sizing_mode="fixed",
            width=350,
            height=50,
        )
        self._filter_select.on_change("value", self.on_change)

        self._words_filter = CheckboxGroup(
            labels=["Filter stopwords", "Keep only hashtags"], active=[0]
        )
        self._words_filter.on_change("active", self.on_change)

        self._date_range_slider = DateRangeSlider(
            title="Time Period",
            start=START_DATE,
            end=START_DATE
            + timedelta(days=TIME_SPAN.days - TIME_SPAN.days % DAY_RESOLUTION),
            step=int(timedelta(days=DAY_RESOLUTION).total_seconds()) * 1000,
            callback_policy="mouseup",
            value=(START_DATE, END_DATE),
            sizing_mode="fixed",
            width=350,
            height=40,
        )
        self._date_range_slider.on_change("value_throttled", self.on_change)

        frequencies_table = DataTable(
            source=self._source,
            columns=[
                TableColumn(field="words", title="Word"),
                TableColumn(field="frequencies", title="Frequency"),
            ],
            selectable=False,
            sizing_mode="stretch_both",
        )

        self.panel = Panel(
            child=row(
                column(
                    description,
                    self._query_select,
                    self._language_select,
                    self._filter_select,
                    self._words_filter,
                    self._date_range_slider,
                    sizing_mode="fixed",
                ),
                frequencies_table,
                sizing_mode="stretch_both",
            ),
            title="Word Frequencies",
        )

    def update(self) -> None:
        start_date, end_date = self._date_range_slider.value_as_date
        if end_date <= start_date:
            end_date = start_date + timedelta(days=1)
        time_span = end_date - start_date

        query = self._query_select.value
        language = self._language_select.value
        filter_ = SearchFilter[self._filter_select.value.upper()]

        time_span_frequencies = Counter[str]()
        for days in range(0, time_span.days, DAY_RESOLUTION):
            current_date = start_date + timedelta(days=days)
            time_span_frequencies.update(
                frequencies[filter_][language][query][current_date]
            )

        new_data: Mapping[str, List[object]] = {"words": [], "frequencies": []}
        for i, (word, frequency) in enumerate(time_span_frequencies.most_common()):
            if 0 in self._words_filter.active and word in STOPWORDS[language]:
                continue
            if 1 in self._words_filter.active and not word.startswith("#"):
                continue

            new_data["words"].append(word)
            new_data["frequencies"].append(frequency)

            if i == TOP_K_MOST_FREQUENT_WORDS:
                break

        self._source.data = new_data

    def on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
