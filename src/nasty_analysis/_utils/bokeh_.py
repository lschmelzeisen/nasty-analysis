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
from typing import Mapping, Optional

from bokeh.application.application import Application, ServerContext
from bokeh.application.handlers import Handler
from bokeh.models import CustomJS, Range1d
from nasty_utils import date_to_timestamp
from overrides import overrides


class ParameterPassingApplication(Application):
    @overrides
    def __init__(
        self,
        *handlers: Handler,
        metadata: Optional[Mapping[str, object]] = None,
        server_context_params: Optional[Mapping[str, object]] = None,
    ):
        super().__init__(*handlers, metadata=metadata)
        self._server_context_params = server_context_params

    @overrides
    def on_server_loaded(self, server_context: ServerContext):
        if self._server_context_params:
            for name, value in self._server_context_params.items():
                setattr(server_context, name, value)

        super().on_server_loaded(server_context)


def bounded_range1d(
    start: float, end: float, min_interval: Optional[float] = None
) -> Range1d:
    # While Range1d has a `bounds` parameter, it does not seem to work when used with
    # RangeTool(). Therefore we control the bounds via JS here.

    result = Range1d(
        start=start,
        end=end,
        bounds=(start, end),
        min_interval=min_interval,
    )
    result.js_on_change(
        "start",
        CustomJS(
            code="""
                var b = this.bounds;
                if (b && b.length == 2) {
                    this.start = Math.min(Math.max(this.start, b[0]), b[1]);
                }
                if (this.min_interval && this.end - this.start < this.min_interval) {
                    this.start = this.end - this.min_interval;
                }
            """
        ),
    )
    result.js_on_change(
        "end",
        CustomJS(
            code="""
                var b = this.bounds;
                if (b && b.length == 2) {
                    this.end = Math.min(Math.max(this.end, b[0]), b[1]);
                }
                if (this.min_interval && this.end - this.start < this.min_interval) {
                    this.end = this.start + this.min_interval;
                }
            """
        ),
    )

    return result


def bounded_date_range1d(
    start: date, end: date, min_interval: Optional[timedelta] = None
) -> Range1d:
    start = date_to_timestamp(start, tzinfo_=timezone.utc) * 1000
    end = date_to_timestamp(end, tzinfo_=timezone.utc) * 1000
    if min_interval:
        min_interval = min_interval.total_seconds() * 1000
    return bounded_range1d(start, end, min_interval)
