import sys

import pydantic
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, CliApp, CliSubCommand

from abertpy import __version__
from abertpy.models import CleanupArgs, PingArgs, ProxyArgs, SetupArgs


class App(BaseSettings, cli_parse_args=True, cli_implicit_flags=True, case_sensitive=True):
    version: bool = Field(
        default=False,
        validation_alias=AliasChoices("V", "version"),
        description="Show the abertpy version and exit.",
    )

    ping: CliSubCommand[PingArgs]
    proxy: CliSubCommand[ProxyArgs]
    setup: CliSubCommand[SetupArgs]
    cleanup: CliSubCommand[CleanupArgs]

    def cli_cmd(self) -> None:
        if self.version:
            print(f"abertpy {__version__}")
            return
        CliApp.run_subcommand(self)


def main() -> None:
    try:
        CliApp.run(App)
    except pydantic.ValidationError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
