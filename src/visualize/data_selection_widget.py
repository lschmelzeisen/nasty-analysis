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

from datetime import date, timedelta
from typing import Callable, Counter, Iterator, Tuple

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


class DataSelectionWidget:
    def __init__(self) -> None:
        self.query_select = Select(
            title="Query:",
            options=QUERIES,
            value=QUERIES[0],
            sizing_mode="stretch_width",
        )
        self.language_select = Select(
            title="Language:",
            options=LANGUAGES,
            value=LANGUAGES[0],
            sizing_mode="stretch_width",
        )
        self.filter_select = Select(
            title="Search Filter:",
            options=[filter_.name.lower() for filter_ in FILTERS],
            value=FILTERS[0].name.lower(),
            sizing_mode="stretch_width",
        )
        self.date_range_slider = DateRangeSlider(
            title="Time Period",
            start=START_DATE_RESOLUTION,
            end=END_DATE_RESOLUTION,
            step=int(timedelta(days=DAY_RESOLUTION).total_seconds()) * 1000,
            value=(START_DATE_RESOLUTION, END_DATE_RESOLUTION),
            sizing_mode="stretch_width",
        )

        self.selection_inputs = [
            self.query_select,
            self.language_select,
            self.filter_select,
            self.date_range_slider,
        ]

    def iter_frequencies_in_selection(self) -> Iterator[Tuple[date, Counter[str]]]:
        start_date, end_date = self.date_range_slider.value_as_date
        if end_date <= start_date:
            end_date = start_date + timedelta(days=1)
        time_span = end_date - start_date

        query = self.query_select.value
        language = self.language_select.value
        filter_ = SearchFilter[self.filter_select.value.upper()]
        for days in range(0, time_span.days, DAY_RESOLUTION):
            current_date = start_date + timedelta(days=days)
            yield current_date, frequencies[filter_][language][query][current_date]

    def register_on_change_func(
        self, on_change_func: Callable[[str, object, object], None]
    ):
        self.query_select.on_change("value", on_change_func)
        self.language_select.on_change("value", on_change_func)
        self.filter_select.on_change("value", on_change_func)
        self.date_range_slider.on_change("value_throttled", on_change_func)
