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


from typing import Callable, Counter, Mapping, Sequence

from bokeh.layouts import column
from bokeh.models import CheckboxGroup, TextInput

from nasty_analysis.serve.widgets.dataset_widget import DatasetWidget
from nasty_analysis.settings import DatasetSection


class DatasetWordsWidget:
    def __init__(
        self,
        datasets: Sequence[DatasetSection],
        lang_freqs_by_dataset: Mapping[str, Counter[str]],
        query_freqs_by_dataset: Mapping[str, Counter[str]],
        url_netloc_freqs_by_dataset: Mapping[str, Counter[str]],
    ):
        self.dataset_widget = DatasetWidget(
            datasets,
            lang_freqs_by_dataset,
            query_freqs_by_dataset,
            url_netloc_freqs_by_dataset,
        )

        self._should_num_docs = CheckboxGroup(
            labels=["Visualize number of documents"], active=[]
        )

        self._words = TextInput(title="Words (space separated):", value="")

        self.widget = column(
            self.dataset_widget.widget,
            self._should_num_docs,
            self._words,
            sizing_mode="stretch_width",
        )

    def on_change(self, *callbacks: Callable[[str, object, object], None]) -> None:
        self.dataset_widget.on_change(*callbacks)
        self._should_num_docs.on_change("active", *callbacks)
        self._words.on_change("value", *callbacks)

    def set_enabled(self, enabled: bool) -> None:
        self.dataset_widget.set_enabled(enabled)
        self._should_num_docs.disabled = not enabled
        self._words.disabled = not enabled

    @property
    def should_num_docs(self) -> bool:
        return bool(self._should_num_docs.active)

    @property
    def words(self) -> Sequence[str]:
        return self._words.value.split()
