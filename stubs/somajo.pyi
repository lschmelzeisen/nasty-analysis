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

from typing import Iterable, Iterator, Sequence

class Token:
    text: str = ...
    token_class: str = ...

class SoMaJo:
    def __init__(
        self,
        language: str,
        *,
        split_camel_case: bool = ...,
        split_sentences: bool = ...,
    ): ...
    def tokenize_text(
        self, paragraphs: Iterable[str], *, parallel: int = ...
    ) -> Iterator[Sequence[Token]]: ...
