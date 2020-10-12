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

from elasticsearch_dsl import Float, Keyword, Text
from nasty_utils import DecompressingTextIOWrapper
from typing_extensions import Final

from nasty_analysis.document.news_csv import NewsCsvDocument

_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "with_positions_offsets"


class MaxqdaCodedNewsCsvDocument(NewsCsvDocument):
    document_id = Keyword()
    document_group = Keyword()
    code_identifier = Keyword()
    code = Keyword()
    segment = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    coverage = Float()

    @classmethod
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)
        if "Farbe" in doc_dict:
            # Else doc_dict would be just for a regular NEWS_CSV document, which we do
            # not want to modify here.
            doc_dict.pop("Farbe")
            doc_dict.pop("Kommentar")
            doc_dict["document_group"] = doc_dict.pop("Dokumentgruppe")
            doc_dict.pop("Dokumentname")
            doc_dict["document_id"] = doc_dict.pop("_id")
            doc_dict["_id"] = (
                str(doc_dict["code_identifier"]) + "-" + str(doc_dict.pop("i"))
            )
            doc_dict["code"] = doc_dict.pop("Code")
            doc_dict["segment"] = doc_dict.pop("Segment")
            doc_dict["coverage"] = float(doc_dict.pop("Abdeckungsgrad %"))


def load_document_dicts_from_maxqda_coded_news_csv(
    file: Path,
    code_identifier: str,
    news_csv_document_dicts: Mapping[str, Mapping[str, object]],
    progress_bar: bool = True,
) -> Iterator[Mapping[str, object]]:
    with DecompressingTextIOWrapper(
        file, encoding="UTF-8", warn_uncompressed=False, progress_bar=progress_bar
    ) as fin:
        reader = csv.DictReader(fin)
        for i, document_dict in enumerate(reader):
            document_dict["i"] = i
            document_dict["code_identifier"] = code_identifier
            document_dict.update(news_csv_document_dicts[document_dict["Dokumentname"]])
            yield document_dict
