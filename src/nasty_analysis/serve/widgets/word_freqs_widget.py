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

from enum import Enum
from typing import Callable

from bokeh.layouts import column
from bokeh.models import CheckboxGroup, RadioGroup


class WordFilter(Enum):
    def __init__(self, label: str):
        self.label = label

    ALL_WORDS = "All words"
    ONLY_NON_STOPWORDS = "Only non-stopwords"
    ONLY_HASHTAGS = "Only #hashtags"
    ONLY_MENTIONS = "Only @mentions"


class WordFreqsWidget:
    def __init__(self):
        self._word_filter_select = RadioGroup(
            labels=[word_filter.label for word_filter in WordFilter],
            active=1,
        )
        self._should_normalize = CheckboxGroup(
            labels=["Normalize word frequencies per day"], active=[]
        )

        self.widget = column(
            self._word_filter_select,
            self._should_normalize,
            sizing_mode="stretch_width",
        )

    def on_change(self, *callbacks: Callable[[str, object, object], None]) -> None:
        self._word_filter_select.on_change("active", *callbacks)
        self._should_normalize.on_change("active", *callbacks)

    def set_enabled(self, enabled: bool) -> None:
        self._word_filter_select.disabled = not enabled
        self._should_normalize.disabled = not enabled

    @property
    def word_filter(self) -> WordFilter:
        for i, word_filter in enumerate(WordFilter):
            if i == self._word_filter_select.active:
                return word_filter
        raise ValueError()

    @property
    def should_normalize(self) -> bool:
        return bool(self._should_normalize.active)
