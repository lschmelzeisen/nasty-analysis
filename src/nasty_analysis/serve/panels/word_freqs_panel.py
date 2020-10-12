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
from nasty_analysis.serve.figures.num_docs_figure import NumDocsFigure
from nasty_analysis.serve.figures.word_freqs_figure import WordFreqsFigure
from nasty_analysis.serve.widgets.dataset_widget import DatasetWidget
from nasty_analysis.serve.widgets.date_range_widget import DateRangeWidget
from nasty_analysis.serve.widgets.word_freqs_widget import WordFreqsWidget


class WordFreqsPanel:
    def __init__(
        self,
        *,
        context: Context,
        add_next_tick_callback: Callable[[Callable[[], None]], None],
    ):
        date_range_widget = DateRangeWidget(context.min_date, context.max_date)
        date_range_widget.on_change(self._on_change)

        dataset_widget = DatasetWidget(
            context.settings.analysis.datasets,
            context.lang_freqs_by_dataset,
            context.query_freqs_by_dataset,
            context.url_netloc_freqs_by_dataset,
        )
        dataset_widget.on_change(self._on_change)

        word_freqs_widget = WordFreqsWidget()
        word_freqs_widget.on_change(self._on_change)

        num_docs_figure = NumDocsFigure(
            context.min_date,
            context.max_date,
            date_range_widget,
        )

        self._word_freqs_figure = WordFreqsFigure(
            context.settings.analysis.serve.word_freqs.top_n_words,
            context.min_date,
            context.max_date,
            date_range_widget,
            dataset_widget,
            word_freqs_widget,
            num_docs_figure,
            add_next_tick_callback,
        )

        self.panel = Panel(
            title="Word Frequencies",
            child=row(
                column(
                    num_docs_figure.figure,
                    row(date_range_widget.widget, margin=(0, 0, 0, 35)),
                    Div(text="<hr/>", style={"width": "100%"}),
                    dataset_widget.widget,
                    Div(text="<hr/>", style={"width": "100%"}),
                    word_freqs_widget.widget,
                    sizing_mode="stretch_height",
                    width=350,
                ),
                self._word_freqs_figure.figure,
                sizing_mode="stretch_both",
            ),
        )

        self.update()

    def update(self) -> None:
        self._word_freqs_figure.update()

    def _on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
