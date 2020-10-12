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
from datetime import datetime
from pathlib import Path
from typing import Iterator, Mapping, MutableMapping

from elasticsearch_dsl import Date, Float, Keyword, Text
from nasty_data import BaseDocument
from nasty_utils import DecompressingTextIOWrapper
from typing_extensions import Final

_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "with_positions_offsets"


class MaxqdaCodedNastyDocument(BaseDocument):
    document_group = Keyword()
    code_identifier = Keyword()
    lang = Keyword()
    created_at = Date()
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
        doc_dict.pop("Farbe")
        doc_dict.pop("Kommentar")
        doc_dict["document_group"] = doc_dict.pop("Dokumentgruppe")
        doc_dict["created_at"] = datetime.strptime(
            doc_dict.pop("Dokumentname"), "%d.%m.%Y %H:%M:%S"
        )
        doc_dict["_id"] = (
            str(doc_dict["code_identifier"]) + "-" + str(doc_dict.pop("i"))
        )
        doc_dict["code"] = doc_dict.pop("Code")
        doc_dict["segment"] = doc_dict.pop("Segment")
        doc_dict["coverage"] = float(doc_dict.pop("Abdeckungsgrad %"))


def load_document_dicts_from_maxqda_coded_nasty_csv(
    file: Path,
    code_identifier: str,
    lang: str,
    progress_bar: bool = True,
) -> Iterator[Mapping[str, object]]:
    with DecompressingTextIOWrapper(
        file, encoding="UTF-8", warn_uncompressed=False, progress_bar=progress_bar
    ) as fin:
        reader = csv.DictReader(fin)
        for i, document_dict in enumerate(reader):
            document_dict["i"] = i
            document_dict["code_identifier"] = code_identifier
            document_dict["lang"] = lang
            yield document_dict
