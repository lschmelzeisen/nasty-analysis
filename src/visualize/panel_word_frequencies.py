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

from typing import Counter, List, Mapping

from bokeh.layouts import column, row
from bokeh.models import (
    CheckboxGroup,
    ColumnDataSource,
    DataTable,
    Div,
    Panel,
    TableColumn,
)
from stopwordsiso import stopwords

from src.config import LANGUAGES, TOP_K_MOST_FREQUENT_WORDS
from src.visualize.data_selection_widget import DataSelectionWidget

STOPWORDS = {language: stopwords(language) for language in LANGUAGES}


class PanelWordFrequencies:
    def __init__(self, data_selection_widget: DataSelectionWidget) -> None:
        super().__init__()

        self._data_selection_widget = data_selection_widget
        self._data_selection_widget.register_on_change_func(self.on_change)

        self._source = ColumnDataSource({"words": [], "frequencies": []})

        description = Div(
            text="""
                <h1>Word Frequencies</h1>
                <p>Shows the number of times each word occurs in a selection of
                Tweets (sorted in descending order).</p>
                {}
                <p>For the displayed word frequencies, you can adjust to filter out
                <em>stop words</em> (words with no meaning, for example "the") or not,
                or to <em>keep only #hashtags</em>.</p>
            """.format(
                self._data_selection_widget.description
            ),
            sizing_mode="stretch_width",
        )

        self._words_filter = CheckboxGroup(
            labels=["Filter stop words", "Keep only hashtags"],
            active=[0],
            sizing_mode="stretch_width",
        )
        self._words_filter.on_change("active", self.on_change)

        frequencies_table = DataTable(
            source=self._source,
            columns=[
                TableColumn(field="words", title="Word"),
                TableColumn(field="frequencies", title="Frequency"),
            ],
            sizing_mode="stretch_both",
        )

        self.panel = Panel(
            child=row(
                column(
                    description,
                    *self._data_selection_widget.selection_inputs,
                    self._words_filter,
                    sizing_mode="stretch_height",
                    width=350,
                ),
                frequencies_table,
                sizing_mode="stretch_both",
            ),
            title="Word Frequencies",
        )

    def update(self) -> None:
        selection_frequencies = Counter[str]()
        for (
            _current_date,
            date_frequencies,
        ) in self._data_selection_widget.iter_frequencies_in_selection():
            selection_frequencies.update(date_frequencies)

        new_data: Mapping[str, List[object]] = {"words": [], "frequencies": []}
        for i, (word, frequency) in enumerate(selection_frequencies.most_common()):
            if i == TOP_K_MOST_FREQUENT_WORDS:
                break

            language = self._data_selection_widget.language_select.value
            if 0 in self._words_filter.active and word in STOPWORDS[language]:
                continue
            if 1 in self._words_filter.active and not word.startswith("#"):
                continue

            new_data["words"].append(word)
            new_data["frequencies"].append(frequency)

        self._source.data = new_data

    def on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
