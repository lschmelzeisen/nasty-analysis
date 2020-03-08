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

from bokeh.models import Tabs
from bokeh.plotting import curdoc

from src.visualize.panel_word_frequencies import PanelWordFrequencies
from src.visualize.panel_word_trends import PanelWordTrends

panel_word_frequencies = PanelWordFrequencies()
panel_word_trends = PanelWordTrends()

doc = curdoc()
doc.title = "ncov-media-analysis"
doc.add_root(Tabs(tabs=[panel_word_frequencies.panel, panel_word_trends.panel]))

panel_word_frequencies.update()
panel_word_trends.update()
