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

import csv
from pathlib import Path
from typing import Iterator, Mapping, MutableMapping
from urllib.parse import urlparse

import dateparser
from elasticsearch_dsl import Date, Keyword, Text
from nasty_data import BaseDocument
from nasty_utils import DecompressingTextIOWrapper, checked_cast
from typing_extensions import Final

_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "with_positions_offsets"


class NewsCsvDocument(BaseDocument):
    lang = Keyword()
    url = Keyword(doc_values=False)
    url_netloc = Keyword()
    url_path = Keyword(multi=True)
    title = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    time = Date()
    text = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )

    @classmethod
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)
        doc_dict["_id"] = doc_dict.pop("index")

        url = urlparse(checked_cast(str, doc_dict["url"]))
        netloc = url.netloc
        if netloc.startswith("www."):
            netloc = netloc[len("www.") :]
        doc_dict["url_netloc"] = netloc
        doc_dict["url_path"] = url.path.strip("/").split("/")

        doc_dict["time"] = dateparser.parse(
            checked_cast(str, doc_dict["time"]), languages=[str(doc_dict["lang"]), "en"]
        )

        doc_dict.pop("kw")


def load_document_dicts_from_news_csv(
    file: Path,
    lang: str = "de",
    progress_bar: bool = True,
) -> Iterator[Mapping[str, object]]:
    with DecompressingTextIOWrapper(
        file, encoding="UTF-8", warn_uncompressed=False, progress_bar=progress_bar
    ) as fin:
        reader = csv.DictReader(fin)
        for document_dict in reader:
            document_dict["lang"] = lang
            yield document_dict
