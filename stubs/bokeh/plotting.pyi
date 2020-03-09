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

from typing import Optional, Sequence

from .models import ColumnDataSource, LinearAxis, Model, Title, Tool

class curdoc:  # noqa: N801
    title: str = ...
    def add_root(self, model: Model) -> None: ...

class figure(Model):  # noqa: N801
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        title: str = ...,
        toolbar_location: str = ...,
        x_axis_label: str = ...,
        y_axis_label: str = ...,
        active_drag: Optional[str] = ...,
    ): ...
    def line(
        self,
        x: str = ...,
        y: str = ...,
        name: str = ...,
        source: ColumnDataSource = ...,
        color: str = ...,
        line_width: int = ...,
    ) -> Model: ...
    def circle(
        self,
        x: str = ...,
        y: str = ...,
        name: str = ...,
        source: ColumnDataSource = ...,
        color: str = ...,
        size: int = ...,
    ) -> Model: ...
    def add_layout(self, obj: Model) -> None: ...
    def add_tools(self, *tools: Tool) -> None: ...
    title: Title = ...
    xaxis: Sequence[LinearAxis] = ...
