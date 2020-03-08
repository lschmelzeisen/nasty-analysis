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

from bokeh.models import Div, Panel


class PanelWordTrends:
    def __init__(self) -> None:
        description = Div(
            text="""
                <h1>Word Trends</h1>
            """,
            sizing_mode="fixed",
            width=350,
        )

        self.panel = Panel(child=description, title="Word Trends")

    def update(self) -> None:
        pass

    def on_change(self, _attr: str, _old: object, _new: object) -> None:
        self.update()
