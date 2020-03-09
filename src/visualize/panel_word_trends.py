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

from datetime import date
from typing import Dict, List, Union

from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    DatetimeTickFormatter,
    Div,
    HoverTool,
    Legend,
    Panel,
    TextInput,
)
from bokeh.palettes import Category10_5
from bokeh.plotting import figure
from overrides import overrides

from src.config import LANGUAGES_NATURAL, NUM_TREND_INPUTS
from src.visualize.abstract_panel_frequencies import AbstractPanelFrequencies


class PanelWordTrends(AbstractPanelFrequencies):
    def __init__(self) -> None:
        super().__init__()

        self._source = ColumnDataSource(
            {"dates": [], **{"trend" + str(i): [] for i in range(NUM_TREND_INPUTS)}}
        )

        description = Div(
            text="""
                <h1>Word Trends</h1>
                <p>To do</p>
            """,
            sizing_mode="stretch_width",
        )

        self._trend_inputs = []
        for i in range(NUM_TREND_INPUTS):
            self._trend_inputs.append(
                TextInput(title="Trend {}:".format(i + 1), sizing_mode="stretch_width")
            )
            self._trend_inputs[-1].on_change("value", self.on_change)

        self._figure = figure(
            title="Word frequencies",
            toolbar_location="above",
            sizing_mode="stretch_both",
            y_axis_label="Word Frequency",
            active_drag=None,
        )
        self._figure.add_tools(
            HoverTool(
                tooltips=[("Date", "@dates{%d %b %Y}"), ("Frequency", "@$name")],
                formatters={"@dates": "datetime"},
            )
        )
        self._figure.xaxis[0].formatter = DatetimeTickFormatter(
            days=["%d %b %Y"], months=["%b %Y"]
        )

        self._lines = []
        self._circles = []
        for i in range(NUM_TREND_INPUTS):
            self._lines.append(
                self._figure.line(
                    x="dates",
                    y="trend" + str(i),
                    name="trend" + str(i),
                    source=self._source,
                    color=Category10_5[i],
                    line_width=3,
                )
            )
            self._circles.append(
                self._figure.circle(
                    x="dates",
                    y="trend" + str(i),
                    name="trend" + str(i),
                    source=self._source,
                    color=Category10_5[i],
                    size=9,
                )
            )

        self._legend = Legend(items=[], location="top_left")
        self._figure.add_layout(self._legend)

        self.panel = Panel(
            child=row(
                column(
                    description,
                    *self._selection_inputs,
                    *self._trend_inputs,
                    sizing_mode="stretch_height",
                    width=350,
                ),
                self._figure,
                sizing_mode="stretch_both",
            ),
            title="Word Trends",
        )

    @overrides
    def update(self) -> None:
        self._figure.title.text = (
            "Word frequencies for {} {} Tweets containing '{}' "
            "between {:%d %b %Y} and {:%d %b %Y}".format(
                self._filter_select.value,
                LANGUAGES_NATURAL[self._language_select.value],
                self._query_select.value,
                *self._date_range_slider.value_as_date,
            )
        )

        new_data: Dict[str, List[Union[date, int]]] = {
            "dates": [],
            **{"trend" + str(i): [] for i in range(NUM_TREND_INPUTS)},
        }
        for current_date, date_frequencies in self._iter_frequencies_in_selection():
            new_data["dates"].append(current_date)
            for i in range(NUM_TREND_INPUTS):
                trend = self._trend_inputs[i].value
                new_data["trend" + str(i)].append(
                    date_frequencies[trend] if trend else 0
                )

        legend_items = []
        for i in range(NUM_TREND_INPUTS):
            line = self._lines[i]
            circle = self._circles[i]
            trend = self._trend_inputs[i].value
            line.visible = bool(trend)
            circle.visible = bool(trend)
            if trend:
                legend_items.append((trend, [line, circle]))
        self._legend.items = legend_items

        self._source.data = new_data
