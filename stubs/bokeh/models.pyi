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
from typing import Callable, Mapping, Sequence, Tuple, Union

class ColumnDataSource:
    def __init__(self, data: Mapping[str, object]): ...
    data: Mapping[str, object] = ...

class Model:
    visible: bool = ...

class CheckboxGroup(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        labels: Sequence[str] = ...,
        active: Sequence[int] = ...,
    ): ...
    def on_change(
        self, attr: str, *callbacks: Callable[[str, object, object], None]
    ) -> None: ...
    active: Sequence[int] = ...

class DateRangeSlider(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        title: str = ...,
        start: date = ...,
        end: date = ...,
        step: int = ...,
        value: Tuple[date, date] = ...,
        callback_policy: str = ...,
    ): ...
    def on_change(
        self, attr: str, *callbacks: Callable[[str, object, object], None]
    ) -> None: ...
    value_as_date: Tuple[date, date] = ...

class Div(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        text: str = ...,
    ): ...

class Panel(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        title: str = ...,
        child: Model = ...,
    ): ...

class Select(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        title: str = ...,
        options: Sequence[str] = ...,
        value: str = ...,
    ): ...
    def on_change(
        self, attr: str, *callbacks: Callable[[str, object, object], None]
    ) -> None: ...
    value: str = ...

class TextInput(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        title: str = ...,
        value: str = ...,
    ): ...
    def on_change(
        self, attr: str, *callbacks: Callable[[str, object, object], None]
    ) -> None: ...
    value: str = ...

class TableColumn(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        field: str = ...,
        title: str = ...,
    ): ...

class Tabs(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        tabs: Sequence[Panel] = ...,
    ): ...

class DataTable(Model):
    def __init__(
        self,
        width: int = ...,
        height: int = ...,
        sizing_mode: str = ...,
        columns: Sequence[TableColumn] = ...,
        source: ColumnDataSource = ...,
    ): ...

class Legend(Model):
    def __init__(
        self, items: Sequence[Tuple[str, Sequence[Model]]] = ..., location: str = ...
    ): ...
    items: Sequence[Tuple[str, Sequence[Model]]] = ...

class DatetimeTickFormatter:
    def __init__(self, days: Sequence[str] = ..., months: Sequence[str] = ...): ...

class LinearAxis:
    formatter: DatetimeTickFormatter = ...

class Title:
    text: str = ...

class Tool(Model): ...

class HoverTool(Tool):
    def __init__(
        self,
        tooltips: Union[str, Sequence[Tuple[str, str]]] = ...,
        formatters: Mapping[str, str] = ...,
    ): ...
