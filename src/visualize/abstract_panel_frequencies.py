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

from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Counter, Iterator, Tuple

from bokeh.models import DateRangeSlider, Select
from nasty import SearchFilter

from src.config import (
    DAY_RESOLUTION,
    END_DATE_RESOLUTION,
    FILTERS,
    LANGUAGES,
    QUERIES,
    START_DATE_RESOLUTION,
)
from src.visualize.frequencies import frequencies


class AbstractPanelFrequencies(ABC):
    _query_select = Select(
        title="Query:", options=QUERIES, value=QUERIES[0], sizing_mode="stretch_width",
    )
    _language_select = Select(
        title="Language:",
        options=LANGUAGES,
        value=LANGUAGES[0],
        sizing_mode="stretch_width",
    )
    _filter_select = Select(
        title="Search Filter:",
        options=[filter_.name.lower() for filter_ in FILTERS],
        value=FILTERS[0].name.lower(),
        sizing_mode="stretch_width",
    )
    _date_range_slider = DateRangeSlider(
        title="Time Period",
        start=START_DATE_RESOLUTION,
        end=END_DATE_RESOLUTION,
        step=int(timedelta(days=DAY_RESOLUTION).total_seconds()) * 1000,
        value=(START_DATE_RESOLUTION, END_DATE_RESOLUTION),
        callback_policy="mouseup",
        sizing_mode="stretch_width",
    )
    _selection_inputs = [
        _query_select,
        _language_select,
        _filter_select,
        _date_range_slider,
    ]

    def __init__(self) -> None:
        self._query_select.on_change("value", self.on_change)
        self._language_select.on_change("value", self.on_change)
        self._filter_select.on_change("value", self.on_change)
        self._date_range_slider.on_change("value_throttled", self.on_change)

    def _iter_frequencies_in_selection(self) -> Iterator[Tuple[date, Counter[str]]]:
        start_date, end_date = self._date_range_slider.value_as_date
        if end_date <= start_date:
            end_date = start_date + timedelta(days=1)
        time_span = end_date - start_date

        query = self._query_select.value
        language = self._language_select.value
        filter_ = SearchFilter[self._filter_select.value.upper()]
        for days in range(0, time_span.days, DAY_RESOLUTION):
            current_date = start_date + timedelta(days=days)
            yield current_date, frequencies[filter_][language][query][current_date]

    @abstractmethod
    def update(self) -> None:
        pass

    def on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
