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

from typing import Callable

from bokeh.layouts import column, row
from bokeh.models import Div, Panel

from nasty_analysis.serve.context import Context
from nasty_analysis.serve.figures.word_trends_figure import WordTrendsFigure
from nasty_analysis.serve.widgets.dataset_words_widget import DatasetWordsWidget
from nasty_analysis.serve.widgets.date_range_widget import DateRangeWidget
from nasty_analysis.serve.widgets.word_trends_widget import WordTrendsWidget


class WordTrendsPanel:
    def __init__(
        self,
        *,
        context: Context,
        add_next_tick_callback: Callable[[Callable[[], None]], None],
    ):
        date_range_widget = DateRangeWidget(context.min_date, context.max_date)
        date_range_widget.on_change(self._on_change)

        dataset_word_widgets = []
        for _ in range(
            context.settings.analysis.serve.word_trends.num_dataset_word_widgets
        ):
            dataset_word_widget = DatasetWordsWidget(
                context.settings.analysis.datasets,
                context.lang_freqs_by_dataset,
                context.query_freqs_by_dataset,
                context.url_netloc_freqs_by_dataset,
            )
            dataset_word_widget.on_change(self._on_change)
            dataset_word_widgets.append(dataset_word_widget)

        word_trends_widget = WordTrendsWidget()
        word_trends_widget.on_change(self._on_change)

        self._word_trends_figure = WordTrendsFigure(
            context.min_date,
            context.max_date,
            date_range_widget,
            dataset_word_widgets,
            word_trends_widget,
            add_next_tick_callback,
        )

        column_children = [date_range_widget.widget]
        for dataset_word_widget in dataset_word_widgets:
            column_children.append((Div(text="<hr/>", style={"width": "100%"})))
            column_children.append(dataset_word_widget.widget)
        column_children.append((Div(text="<hr/>", style={"width": "100%"})))
        column_children.append(word_trends_widget.widget)

        self.panel = Panel(
            title="Word Trends",
            child=row(
                column(*column_children, sizing_mode="stretch_height", width=350),
                self._word_trends_figure.figure,
                sizing_mode="stretch_both",
            ),
        )

        self.update()

    def update(self) -> None:
        self._word_trends_figure.update()

    def _on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
