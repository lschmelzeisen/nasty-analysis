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
from typing import Dict, List, Union

from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    DateRangeSlider,
    DatetimeTickFormatter,
    Div,
    Legend,
    Panel,
    Select,
    TextInput,
)
from bokeh.palettes import Category10_5
from bokeh.plotting import figure
from nasty import SearchFilter

from src.config import (
    DAY_RESOLUTION,
    FILTERS,
    LANGUAGES,
    NUM_TREND_INPUTS,
    QUERIES,
    START_DATE,
    TIME_SPAN,
)
from src.visualize.frequencies import frequencies


class PanelWordTrends:
    def __init__(self) -> None:
        self._source = ColumnDataSource(
            {"dates": [], **{"trend" + str(i): [] for i in range(NUM_TREND_INPUTS)}}
        )

        description = Div(
            text="""
                <h1>Word Trends</h1>
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

        self._trend_inputs = []
        for i in range(NUM_TREND_INPUTS):
            self._trend_inputs.append(
                TextInput(
                    title="Trend {}:".format(i + 1),
                    sizing_mode="fixed",
                    width=350,
                    height=50,
                )
            )
            self._trend_inputs[-1].on_change("value", self.on_change)

        end_date = START_DATE + timedelta(
            days=TIME_SPAN.days - TIME_SPAN.days % DAY_RESOLUTION
        )
        self._date_range_slider = DateRangeSlider(
            title="Time Period",
            start=START_DATE,
            end=end_date,
            step=int(timedelta(days=DAY_RESOLUTION).total_seconds()) * 1000,
            callback_policy="mouseup",
            value=(START_DATE, end_date),
            sizing_mode="fixed",
            width=350,
            height=40,
        )
        self._date_range_slider.on_change("value_throttled", self.on_change)

        figure_ = figure(
            title="Word Trends", toolbar_location="above", sizing_mode="stretch_both",
        )
        print(type(figure_.xaxis[0]))
        figure_.xaxis[0].formatter = DatetimeTickFormatter(
            days=["%d %b %Y"], months=["%b %Y"]
        )
        self._lines = []
        for i in range(NUM_TREND_INPUTS):
            self._lines.append(
                figure_.line(
                    x="dates",
                    y="trend" + str(i),
                    source=self._source,
                    color=Category10_5[i],
                    line_width=3,
                )
            )

        self._legend = Legend(items=[], location="top_left")
        figure_.add_layout(self._legend)

        self.panel = Panel(
            child=row(
                column(
                    description,
                    self._query_select,
                    self._language_select,
                    self._filter_select,
                    *self._trend_inputs,
                    self._date_range_slider,
                    sizing_mode="fixed",
                ),
                figure_,
                sizing_mode="stretch_both",
            ),
            title="Word Trends",
        )

    def update(self) -> None:
        start_date, end_date = self._date_range_slider.value_as_date
        if end_date <= start_date:
            end_date = start_date + timedelta(days=1)
        time_span = end_date - start_date

        query = self._query_select.value
        language = self._language_select.value
        filter_ = SearchFilter[self._filter_select.value.upper()]

        new_data: Dict[str, List[Union[date, int]]] = {
            "dates": [],
            **{"trend" + str(i): [] for i in range(NUM_TREND_INPUTS)},
        }

        for days in range(0, time_span.days, DAY_RESOLUTION):
            current_date = start_date + timedelta(days=days)
            new_data["dates"].append(current_date)
            for i in range(NUM_TREND_INPUTS):
                trend = self._trend_inputs[i].value
                new_data["trend" + str(i)].append(
                    frequencies[filter_][language][query][current_date][trend]
                    if trend
                    else 0
                )

        legend_items = []
        for i in range(NUM_TREND_INPUTS):
            line = self._lines[i]
            trend = self._trend_inputs[i].value
            line.visible = bool(trend)
            if trend:
                legend_items.append((trend, [line]))
        self._legend.items = legend_items

        self._source.data = new_data

    def on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
