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

from typing import (
    AbstractSet,
    Callable,
    Counter,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
)

from bokeh.layouts import column, row
from bokeh.models import Button, CheckboxGroup, Select, TextInput
from elasticsearch_dsl import Search
from nasty import SearchFilter

from nasty_analysis.search_helper import SearchHelper
from nasty_analysis.settings import DatasetSection, DatasetType


class DatasetWidget:
    def __init__(
        self,
        datasets: Sequence[DatasetSection],
        lang_freqs_by_dataset: Mapping[str, Counter[str]],
        query_freqs_by_dataset: Mapping[str, Counter[str]],
        url_netloc_freqs_by_dataset: Mapping[str, Counter[str]],
    ):
        self._datasets = datasets
        self._lang_freqs = lang_freqs_by_dataset
        self._query_freqs = query_freqs_by_dataset
        self._url_netloc_freqs = url_netloc_freqs_by_dataset

        self.dataset = self._datasets[0]

        self._hide_button = Button(
            label="+", button_type="primary", sizing_mode="fixed", width=30, height=30
        )
        self._hide_button.on_click(lambda _event: self._on_click_hide_button())

        # General:
        self._dataset_name = Select(
            title="Dataset:",
            options=[dataset.name for dataset in self._datasets],
            value=self._datasets[0].name,
        )
        self._dataset_name.on_change(
            "value", lambda _attr, _old, _new: self._on_change_name()
        )
        self._lang = Select(title="Language:")
        self._cooccur_words = TextInput(
            title="Words documents must contain (space separated):",
            value="",
        )

        # NASTY-specific:
        self._search_filter = Select(
            title="Search filter:",
            options=["*"] + [search_filter.name for search_filter in SearchFilter],
            value="*",
        )
        self._search_query = Select(title="Search query:")
        self._user_verified = Select(
            title="User verified:", options=["*", str(True), str(False)], value="*"
        )

        # NEWS_CSV-specific:
        self._url_netloc = Select(title="Domain:")

        # MAXQDA_CODED_NEWS_CSV-specific:
        self._code_identifier = Select(title="Code identifier:")
        self._cumulate_subcodes = CheckboxGroup(
            labels=["Cumulate subcodes"], active=[1]
        )

        self._type_specific_widgets = {
            DatasetType.NASTY: column(
                row(
                    self._search_filter, self._search_query, sizing_mode="stretch_width"
                ),
                self._user_verified,
                sizing_mode="stretch_width",
            ),
            DatasetType.NEWS_CSV: column(self._url_netloc, sizing_mode="stretch_width"),
            DatasetType.MAXQDA_CODED_NASTY: column(
                self._code_identifier,
                self._cumulate_subcodes,
                sizing_mode="stretch_width",
            ),
            DatasetType.MAXQDA_CODED_NEWS_CSV: column(
                self._url_netloc,
                self._code_identifier,
                self._cumulate_subcodes,
                sizing_mode="stretch_width",
            ),
        }

        self._collapsed_widget = self._dataset_name
        self._expanded_widget = column(
            self._dataset_name,
            self._lang,
            row(),  # Dummy value to be replaced in self._on_change_name()
            self._cooccur_words,
            sizing_mode="stretch_width",
        )

        self.widget = row(
            self._hide_button, self._collapsed_widget, sizing_mode="stretch_width"
        )

        self._callbacks: MutableSequence[Callable[[str, object, object], None]] = []

        self._on_change_name()

    def _on_change_name(self) -> None:
        for dataset in self._datasets:
            if dataset.name == self._dataset_name.value:
                self.dataset = dataset
                break
        else:
            raise ValueError("Selected dataset does not exist.")

        if self._callbacks:
            self._lang.remove_on_change("value", *self._callbacks)
            self._search_query.remove_on_change("value", *self._callbacks)
            self._url_netloc.remove_on_change("value", *self._callbacks)
            self._code_identifier.remove_on_change("value", *self._callbacks)

        self._lang.options = [
            lang for lang, _freq in self._lang_freqs[self.dataset.name].most_common()
        ]
        self._lang.value = self._lang.options[0]

        if self.dataset.type == DatasetType.NASTY:
            self._search_query.options = ["*"] + [
                q for q, _f in self._query_freqs[self.dataset.name].most_common()
            ]
            self._search_query.value = self._search_query.options[0]

        elif (
            self.dataset.type == DatasetType.NEWS_CSV
            or self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            self._url_netloc.options = ["*"] + [
                u for u, _f in self._url_netloc_freqs[self.dataset.name].most_common()
            ]
            self._url_netloc.value = self._url_netloc.options[0]

        if (
            self.dataset.type == DatasetType.MAXQDA_CODED_NASTY
            or self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            self._code_identifier.options = [
                "*"
            ] + self._build_code_identifier_options()
            self._code_identifier.value = self._code_identifier.options[0]

        if self._callbacks:
            self._lang.on_change("value", *self._callbacks)
            self._search_query.on_change("value", *self._callbacks)
            self._url_netloc.on_change("value", *self._callbacks)
            self._code_identifier.on_change("value", *self._callbacks)

        self._expanded_widget.children[2] = self._type_specific_widgets[
            self.dataset.type
        ]

    def _build_code_identifier_options(self) -> Sequence[str]:
        if self.dataset.type == DatasetType.MAXQDA_CODED_NASTY:
            source = self.dataset.source_maxqda_coded_nasty
        elif self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV:
            source = self.dataset.source_maxqda_coded_news_csv
        else:
            raise NotImplementedError()
        assert source

        codes_stack = []
        for code in reversed(source.codes):
            codes_stack.append((code, 0))

        code_identifier_options = []
        while codes_stack:
            code, depth = codes_stack.pop()
            code_identifier_options.append(
                4 * depth * "\N{NO-BREAK SPACE}" + code.code_identifier
            )

            for c in reversed(code.codes or []):
                codes_stack.append((c, depth + 1))

        return code_identifier_options

    def _on_click_hide_button(self) -> None:
        if self._hide_button.label == "+":
            self._hide_button.label = "−"
            self.widget.children[1] = self._expanded_widget
        else:
            self._hide_button.label = "+"
            self.widget.children[1] = self._collapsed_widget

    def on_change(self, *callbacks: Callable[[str, object, object], None]) -> None:
        self._callbacks.extend(callbacks)

        # General:
        self._dataset_name.on_change("value", *callbacks)
        self._lang.on_change("value", *callbacks)
        self._cooccur_words.on_change("value", *callbacks)

        # NASTY-specific:
        self._search_filter.on_change("value", *callbacks)
        self._search_query.on_change("value", *callbacks)
        self._user_verified.on_change("value", *callbacks)

        # NEWS_CSV-specific:
        self._url_netloc.on_change("value", *callbacks)

        # MAXQDA_CODED_NEWS_CSV-specific:
        self._code_identifier.on_change("value", *callbacks)
        self._cumulate_subcodes.on_change("active", *callbacks)

    def set_enabled(self, enabled: bool) -> None:
        self._dataset_name.disabled = not enabled
        self._lang.disabled = not enabled
        self._cooccur_words.disabled = not enabled

        # NASTY-specific:
        self._search_filter.disabled = not enabled
        self._search_query.disabled = not enabled
        self._user_verified.disabled = not enabled

        # NEWS_CSV-specific:
        self._url_netloc.disabled = not enabled

        # MAXQDA_CODED_NEWS_CSV-specific:
        self._code_identifier.disabled = not enabled
        self._cumulate_subcodes.disabled = not enabled

    @property
    def lang(self) -> str:
        return self._lang.value

    @property
    def cooccur_words(self) -> Sequence[str]:
        return self._cooccur_words.value.split()

    @property
    def search_filter(self) -> Optional[SearchFilter]:
        assert self.dataset.type == DatasetType.NASTY
        if self._search_filter.value == "*":
            return None
        return SearchFilter[self._search_filter.value]

    @property
    def search_query(self) -> Optional[str]:
        assert self.dataset.type == DatasetType.NASTY
        if self._search_query.value == "*":
            return None
        return self._search_query.value

    @property
    def user_verified(self) -> Optional[bool]:
        assert self.dataset.type == DatasetType.NASTY
        if self._user_verified.value == "*":
            return None
        return self._user_verified.value == "True"

    @property
    def url_netloc(self) -> Optional[str]:
        assert (
            self.dataset.type == DatasetType.NEWS_CSV
            or self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        )
        if self._url_netloc.value == "*":
            return None
        return self._url_netloc.value

    @property
    def code_identifier(self) -> AbstractSet[str]:
        if self.dataset.type == DatasetType.MAXQDA_CODED_NASTY:
            source = self.dataset.source_maxqda_coded_nasty
        elif self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV:
            source = self.dataset.source_maxqda_coded_news_csv
        else:
            raise NotImplementedError()
        assert source

        code_identifier = self._code_identifier.value.lstrip("\N{NO-BREAK SPACE}")
        if code_identifier != "*" and 0 not in self._cumulate_subcodes.active:
            return {code_identifier}

        codes_stack = list(source.codes)
        if code_identifier != "*":
            while codes_stack:
                code = codes_stack.pop()
                if code.code_identifier == code_identifier:
                    codes_stack = [code]
                    break
                codes_stack.extend(code.codes or [])
            else:
                raise ValueError(
                    f"Selected code identifier '{code_identifier}' could not be found "
                    "in settings. This is an implementation bug because in this case, "
                    "the code identifier should not be selectable in the GUI."
                )

        i = 0
        while i != len(codes_stack):
            codes_stack.extend(codes_stack[i].codes or [])
            i += 1

        return {code.code_identifier for code in codes_stack}

    def set_search(self, search: Search) -> Search:
        search_helper = SearchHelper(self.dataset.type)

        search = search.index(self.dataset.index).filter(
            search_helper.query_lang_term(self._lang.value)
        )
        for cooccur_word in self.cooccur_words:
            search = search.filter(search_helper.query_text_tokens_term(cooccur_word))

        if self.dataset.type == DatasetType.NASTY:
            if self.search_filter:
                search = search.filter(
                    search_helper.query_nasty_filter_term(self.search_filter.name)
                )
            if self.search_query:
                search = search.filter(
                    search_helper.query_nasty_query_term(self.search_query)
                )
            if self.user_verified:
                search = search.filter(
                    search_helper.query_nasty_user_verified_term(self.user_verified)
                )

        elif (
            self.dataset.type == DatasetType.NEWS_CSV
            or self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            if self.url_netloc:
                search = search.filter(
                    search_helper.query_news_csv_url_netloc_term(self.url_netloc)
                )

        if (
            self.dataset.type == DatasetType.MAXQDA_CODED_NASTY
            or self.dataset.type == DatasetType.MAXQDA_CODED_NEWS_CSV
        ):
            search = search.filter(
                search_helper.query_maxqda_coded_code_identifier_terms(
                    list(self.code_identifier)
                )
            )

        return search
