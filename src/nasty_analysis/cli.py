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
from pathlib import Path
from typing import Optional

from bokeh.application.handlers import DirectoryHandler
from bokeh.command.util import report_server_init_errors
from bokeh.server.server import Server
from nasty_utils import (
    Argument,
    ArgumentGroup,
    ColoredBraceStyleAdapter,
    Program,
    ProgramConfig,
)
from overrides import overrides
from tornado.autoreload import watch

import nasty_analysis
from nasty_analysis import serve
from nasty_analysis._utils.bokeh_ import ParameterPassingApplication
from nasty_analysis.dataset import Dataset
from nasty_analysis.settings import NastyAnalysisSettings

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


def _make_dataset(settings: NastyAnalysisSettings, name: Optional[str]) -> Dataset:
    datasets = settings.analysis.datasets

    if not name:
        msg = "Specify one of the following datasets with the -d, --dataset argument: "
        if not datasets:
            msg += "No datasets configured yet. Add them to the config file."
        else:
            msg += "'" + "', '".join(d.name for d in datasets) + "'"
        raise ValueError(msg)

    for dataset_settings in datasets:
        if dataset_settings.name == name:
            return Dataset(
                dataset_settings,
                max_retries=settings.elasticsearch.max_retries,
                num_procs=settings.analysis.num_procs,
            )

    raise ValueError(f"No dataset with name '{name}' configured.")


_RETRIEVE_ARGUMENT_GROUP = ArgumentGroup(name="Retrieve Arguments")


class _RetrieveProgram(Program):
    class Config(ProgramConfig):
        title = "retrieve"
        aliases = ("r",)
        description = "Retrieve a dataset."

    settings: NastyAnalysisSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    dataset: Optional[str] = Argument(
        short_alias="d",
        description="Name of the dataset.",
        metavar="NAME",
        group=_RETRIEVE_ARGUMENT_GROUP,
    )

    @overrides
    def run(self) -> None:
        dataset = _make_dataset(self.settings, self.dataset)
        dataset.retrieve()


_INDEX_ARGUMENT_GROUP = ArgumentGroup(name="Index Arguments")


class _IndexProgram(Program):
    class Config(ProgramConfig):
        title = "index"
        aliases = ("i",)
        description = "Index a dataset into Elasticsearch."

    settings: NastyAnalysisSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    dataset: Optional[str] = Argument(
        short_alias="d",
        description="Name of the dataset.",
        metavar="NAME",
        group=_INDEX_ARGUMENT_GROUP,
    )

    @overrides
    def run(self) -> None:
        dataset = _make_dataset(self.settings, self.dataset)
        self.settings.setup_elasticsearch_connection()
        dataset.index()


_EXPORT_ARGUMENT_GROUP = ArgumentGroup(name="Export Arguments")


class _ExportProgram(Program):
    class Config(ProgramConfig):
        title = "export"
        aliases = ("e",)
        description = "Export a dataset subset to CSV."

    settings: NastyAnalysisSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    dataset: Optional[str] = Argument(
        short_alias="d",
        description="Name of the dataset.",
        metavar="NAME",
        group=_INDEX_ARGUMENT_GROUP,
    )
    query: str = Argument(
        short_alias="q",
        description="Elasticsearch query string for the exported subset.",
        group=_EXPORT_ARGUMENT_GROUP,
    )
    output: Path = Argument(
        short_alias="o",
        description="CSV-File to which the output will be written.",
        metavar="FILE",
        group=_EXPORT_ARGUMENT_GROUP,
    )

    @overrides
    def run(self) -> None:
        dataset = _make_dataset(self.settings, self.dataset)
        self.settings.setup_elasticsearch_connection()
        dataset.export(self.query, self.output)


_SERVE_ARGUMENTS_GROUP = ArgumentGroup(name="Serve Arguments")


class _ServeProgram(Program):
    class Config(ProgramConfig):
        title = "serve"
        aliases = ("s",)
        description = "Start Bokeh visualization server."

    settings: NastyAnalysisSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    show: bool = Argument(
        False,
        short_alias="s",
        description="Open visualization server in a browser after startup.",
        group=_SERVE_ARGUMENTS_GROUP,
    )
    develop: bool = Argument(
        False,
        short_alias="develop",
        description="Run in development mode (autoreload scripts).",
        group=_SERVE_ARGUMENTS_GROUP,
    )

    @overrides
    def run(self) -> None:
        self.settings.setup_elasticsearch_connection()

        # The following is a simpler `bokeh serve src/nasty_analysis/visualization`.
        # Code for that is in `bokeh.commands.subcommands.serve.Serve.invoke`.
        # Also Bokeh provides this example:
        # https://github.com/bokeh/bokeh/blob/2.0.2/examples/howto/server_embed/standalone_embed.py

        address = self.settings.analysis.serve.address
        port = self.settings.analysis.serve.port
        num_procs = self.settings.analysis.num_procs
        autoreload = False

        if self.develop:
            num_procs = 1
            autoreload = True

            watch(str(self.settings.find_settings_file()))

            for file in Path(nasty_analysis.__file__).parent.glob("**/*.js"):
                watch(str(file))

        application = ParameterPassingApplication(
            DirectoryHandler(filename=Path(serve.__file__).parent),
            server_context_params={"settings": self.settings},
        )
        with report_server_init_errors(address=address, port=port):
            server = Server(
                {"/": application},
                address=address,
                port=port,
                allow_websocket_origin=[f"{address}:{port}"],
                num_procs=num_procs,
                autoreload=autoreload,
            )
            server.start()

            if self.show:
                server.io_loop.add_callback(server.show, "/")
            server.run_until_shutdown()


_GDELT_ARGUMENT_GROUP = ArgumentGroup(name="GDELT Arguments")


class _GdeltProgram(Program):
    # TODO: integrate into RetrieveProgram.
    class Config(ProgramConfig):
        title = "gdelt"
        aliases = ("g",)
        description = "TODO"

    settings: NastyAnalysisSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    @overrides
    def run(self) -> None:
        from nasty_analysis.gdelt import gdelt

        gdelt()


class NastyAnalysisProgram(Program):
    class Config(ProgramConfig):
        title = "nasty-analysis"
        version = nasty_analysis.__version__
        description = "TODO"
        subprograms = (
            _RetrieveProgram,
            _IndexProgram,
            _ExportProgram,
            _ServeProgram,
            _GdeltProgram,
        )
