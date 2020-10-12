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

from datetime import date, timedelta, timezone
from typing import List, Mapping, Sequence

from bokeh.layouts import row
from bokeh.models import (
    BoxAnnotation,
    ColumnDataSource,
    DatetimeTickFormatter,
    NumeralTickFormatter,
)
from bokeh.plotting import Figure
from nasty_utils import date_range, date_to_datetime, date_to_timestamp
from tornado.gen import coroutine

from nasty_analysis.serve.widgets.date_range_widget import DateRangeWidget


class NumDocsFigure:
    def __init__(
        self,
        min_date: date,
        max_date: date,
        date_range_widget: DateRangeWidget,
    ):
        self._min_date = min_date
        self._max_date = max_date

        self._source = ColumnDataSource(self._new_source_data())

        self._figure = Figure(
            title="# Documents per day",
            toolbar_location="above",
            tools="save",
            sizing_mode="stretch_width",
            plot_height=200,
            x_axis_type="datetime",
            y_axis_type="linear",
            x_range=(
                date_to_timestamp(self._min_date, tzinfo_=timezone.utc) * 1000,
                date_to_timestamp(self._max_date, tzinfo_=timezone.utc) * 1000,
            ),
            min_border_left=40,
        )
        self._figure.toolbar.autohide = True
        self._figure.toolbar.logo = None
        self._figure.xaxis[0].formatter = DatetimeTickFormatter(
            days=["%d %b"], months=["%b"]
        )
        self._figure.yaxis[0].formatter = NumeralTickFormatter(format="0a")
        self._figure.vbar(
            source=self._source,
            x="days",
            top="num_docs",
            width=timedelta(days=1).total_seconds() * 1000,
        )

        self._selected_annotation = BoxAnnotation(
            left=self._figure.x_range.start,
            right=self._figure.x_range.end,
            fill_color="gray",
            line_color="darkgray",
            line_alpha=1,
        )
        date_range_widget.slider.js_link(
            "value", self._selected_annotation, "left", attr_selector=0
        )
        date_range_widget.slider.js_link(
            "value", self._selected_annotation, "right", attr_selector=1
        )
        self._figure.add_layout(self._selected_annotation)

        self.figure = row(self._figure)

    @classmethod
    def _new_source_data(cls) -> Mapping[str, List[object]]:
        return {"days": [], "num_docs": []}

    @coroutine
    def display_update(self, num_docs_per_day: Sequence[int]) -> None:
        new_data = self._new_source_data()
        for day, num_docs in zip(
            date_range(self._min_date, self._max_date), num_docs_per_day
        ):
            if not num_docs:
                continue
            new_data["days"].append(date_to_datetime(day, tzinfo_=timezone.utc))
            new_data["num_docs"].append(num_docs)
        self._source.data = new_data
