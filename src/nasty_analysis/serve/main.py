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

from logging import getLogger
from typing import cast

from bokeh.models import Tabs
from bokeh.plotting import curdoc
from nasty_utils import ColoredBraceStyleAdapter

from nasty_analysis.serve.context import Context
from nasty_analysis.serve.panels.word_freqs_panel import WordFreqsPanel
from nasty_analysis.serve.panels.word_trends_panel import WordTrendsPanel

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_LOGGER.debug("Initializing new visualization app document.")

doc = curdoc()
doc.title = "nasty-analysis"

context = cast(Context, getattr(doc.session_context.server_context, "context"))

word_freqs_panel = (
    WordFreqsPanel(context=context, add_next_tick_callback=doc.add_next_tick_callback)
    if context.settings.analysis.serve.word_freqs.enabled
    else None
)

word_trends_panel = (
    WordTrendsPanel(context=context, add_next_tick_callback=doc.add_next_tick_callback)
    if context.settings.analysis.serve.word_trends.enabled
    else None
)

doc.add_root(
    Tabs(
        tabs=[x.panel for x in filter(None, (word_freqs_panel, word_trends_panel))],
        sizing_mode="stretch_both",
    )
)
