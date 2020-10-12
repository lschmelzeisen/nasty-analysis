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
from typing import (
    Callable,
    ClassVar,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from unicodedata import normalize

from elasticsearch_dsl import Document, Field, InnerDoc, Keyword, Object, Text
from lxml import html
from lxml.etree import LxmlError
from nasty_data import (
    BaseDocument,
    NastyBatchResultsTwitterDocument,
    PushshiftRedditDocument,
    customize_document_cls,
)
from nasty_utils import ColoredBraceStyleAdapter, checked_cast
from overrides import overrides
from somajo import SoMaJo
from typing_extensions import Final

from nasty_analysis.document.maxqda_coded_nasty import MaxqdaCodedNastyDocument
from nasty_analysis.document.maxqda_coded_news_csv import MaxqdaCodedNewsCsvDocument
from nasty_analysis.document.news_csv import NewsCsvDocument

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_T_DocumentMeta = Union[Type[Document], Type[InnerDoc]]


class TokenizedBaseDocument(BaseDocument):
    _tokenizers: ClassVar[Mapping[str, SoMaJo]] = {
        "en": SoMaJo("en_PTB", split_sentences=False),
        "de": SoMaJo("de_CMC", split_sentences=False),
    }
    _lang_callback: ClassVar[Callable[[Mapping[str, object]], str]] = lambda _: "en"
    _text_field_map: ClassVar[Mapping[str, object]]

    @classmethod
    def _make_text_field_map(
        cls,
        document_cls: _T_DocumentMeta,
    ) -> Mapping[str, object]:
        text_field_map: MutableMapping[str, object] = {}

        mapping = document_cls._doc_type.mapping
        for field_name in mapping:
            field = mapping[field_name]

            if isinstance(field, Text):
                text_field_map[field_name] = True

            elif isinstance(field, Object):
                inner_class = field._doc_class
                inner_text_field_map = cls._make_text_field_map(inner_class)
                if inner_text_field_map:
                    text_field_map[field_name] = inner_text_field_map

        return text_field_map

    @classmethod
    @overrides
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)

        lang = cls._lang_callback(doc_dict)
        if lang not in cls._tokenizers.keys():
            _LOGGER.error(
                "No tokenizer available for language '{}'. Defaulting to '{}'. "
                "Available languages: {}",
                lang,
                "en",
                ", ".join(cls._tokenizers.keys()),
            )
            lang = "en"

        cls._tokenize_doc_dict(doc_dict, cls._text_field_map, lang)

    @classmethod
    def _tokenize_doc_dict(
        cls,
        doc_dict: MutableMapping[str, object],
        text_field_map: Mapping[str, object],
        lang: str,
    ) -> None:
        for field_name, text_field_or_childs in text_field_map.items():
            # text_field_or_childs is either True or a mapping
            value = doc_dict.get(field_name)
            if not value:
                continue
            elif text_field_or_childs is True:
                (
                    doc_dict[field_name],
                    doc_dict[field_name + "_orig"],
                    doc_dict[field_name + "_tokens"],
                ) = cls._tokenize(checked_cast(str, value), lang)
            elif isinstance(value, MutableMapping):
                cls._tokenize_doc_dict(
                    value, cast(Mapping[str, object], text_field_or_childs), lang
                )
            elif isinstance(value, Sequence):
                for v in value:
                    cls._tokenize_doc_dict(
                        v, cast(Mapping[str, object], text_field_or_childs), lang
                    )
            else:
                raise ValueError(
                    f"Value for Object-field needs to be either a Mapping or a "
                    f"Sequence. The value was: {value}"
                )

    @classmethod
    def _tokenize(cls, text_orig: str, lang: str) -> Tuple[str, str, Sequence[str]]:
        text = text_orig.strip()
        text = normalize("NFKC", text)
        if not text:
            return "", "", []

        try:
            text = str(html.fromstring(text).text_content())
        except LxmlError:
            _LOGGER.warning(
                "lxml HTML parsing failed. Skipping it for this document.",
                exc_info=True,
            )

        if not text:
            return "", "", []

        tokens = [
            token.text.lower()
            for token in next(cls._tokenizers[lang].tokenize_text([text]))
            if (token.token_class not in ["URL", "symbol"])
        ]
        return " ".join(tokens), text_orig, tokens


_T_BaseDocument = TypeVar("_T_BaseDocument", bound=BaseDocument)


def tokenize_document_cls(
    document_cls: Type[_T_BaseDocument],
    *,
    name_prefix: str = "Tokenized",
    text_parameters: Optional[Mapping[str, object]] = None,
    lang_callback: Optional[Callable[[Mapping[str, object]], str]] = None,
) -> Type[_T_BaseDocument]:
    def field_callback(
        _document_cls: Union[Type[Document], Type[InnerDoc]],
        field_name: str,
        field: Field,
        _inner_class: Optional[Type[InnerDoc]],
    ) -> Mapping[str, Field]:
        if isinstance(field, Text):
            return {
                field_name: Text(**(text_parameters or {"analyzer": "whitespace"})),
                field_name + "_orig": Keyword(doc_values=False, index=False),
                field_name + "_tokens": Keyword(),
            }
        return {}

    result: Type[_T_BaseDocument] = customize_document_cls(
        document_cls,
        field_callback,
        name_prefix=name_prefix,
        superclasses=(TokenizedBaseDocument,),
        recursive=True,
    )
    tokenized_result = cast(Type[TokenizedBaseDocument], result)
    tokenized_result._text_field_map = tokenized_result._make_text_field_map(result)
    if lang_callback:
        tokenized_result._lang_callback = lang_callback

    return result


_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "with_positions_offsets"


def _lang_from_field(doc_dict: Mapping[str, object]) -> str:
    lang = checked_cast(str, doc_dict["lang"])
    if lang not in TokenizedBaseDocument._tokenizers.keys():
        return "en"
    return lang


class TokenizedPushshiftRedditDocument(
    tokenize_document_cls(  # type: ignore
        PushshiftRedditDocument,
        text_parameters={
            "index_options": _INDEX_OPTIONS,
            "index_phrases": _INDEX_PHRASES,
            "term_vector": _INDEX_TERM_VECTOR,
            "analyzer": "whitespace",
        },
    )
):
    pass


class TokenizedNastyBatchResultsTwitterDocument(
    tokenize_document_cls(  # type: ignore
        NastyBatchResultsTwitterDocument,
        text_parameters={
            "index_options": _INDEX_OPTIONS,
            "index_phrases": _INDEX_PHRASES,
            "term_vector": _INDEX_TERM_VECTOR,
            "analyzer": "whitespace",
        },
        lang_callback=_lang_from_field,
    )
):
    pass


class TokenizedNewsCsvDocument(
    tokenize_document_cls(  # type: ignore
        NewsCsvDocument,
        text_parameters={
            "index_options": _INDEX_OPTIONS,
            "index_phrases": _INDEX_PHRASES,
            "term_vector": _INDEX_TERM_VECTOR,
            "analyzer": "whitespace",
        },
        lang_callback=_lang_from_field,
    )
):
    pass


class TokenizedMaxqdaCodedNastyDocument(
    tokenize_document_cls(  # type: ignore
        MaxqdaCodedNastyDocument,
        text_parameters={
            "index_options": _INDEX_OPTIONS,
            "index_phrases": _INDEX_PHRASES,
            "term_vector": _INDEX_TERM_VECTOR,
            "analyzer": "whitespace",
        },
        lang_callback=_lang_from_field,
    )
):
    pass


class TokenizedMaxqdaCodedNewsCsvDocument(
    tokenize_document_cls(  # type: ignore
        MaxqdaCodedNewsCsvDocument,
        text_parameters={
            "index_options": _INDEX_OPTIONS,
            "index_phrases": _INDEX_PHRASES,
            "term_vector": _INDEX_TERM_VECTOR,
            "analyzer": "whitespace",
        },
        lang_callback=_lang_from_field,
    )
):
    pass
