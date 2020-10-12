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

from datetime import date, timedelta
from typing import Callable, Tuple

from bokeh.layouts import row
from bokeh.models import DateRangeSlider


class DateRangeWidget:
    def __init__(self, min_date: date, max_date: date):
        self._min_date = min_date
        self._max_date = max_date

        self.slider = DateRangeSlider(
            title="Time Period",
            start=self._min_date,
            end=self._max_date,
            value=(self._min_date, self._max_date),
            step=int(timedelta(days=1).total_seconds()) * 1000,
            sizing_mode="stretch_width",
        )
        self.widget = row(self.slider)

    def on_change(self, *callbacks: Callable[[str, object, object], None]) -> None:
        self.slider.on_change("value_throttled", *callbacks)

    def set_enabled(self, enabled: bool) -> None:
        self.slider.disabled = not enabled

    @property
    def min_and_max_date(self) -> Tuple[date, date]:
        return self.slider.value_as_date
