from typing import Annotated, Optional

import typer
from pydantic_typer import Typer

from abertpy import __version__
from abertpy.ping import app as ping_app
from abertpy.proxy import app as proxy_app
from abertpy.setup import app as setup_app

app = Typer()


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"abertpy {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        Optional[bool],
        typer.Option(
            "--version",
            "-V",
            callback=_version_callback,
            is_eager=True,
            help="Show the abertpy version and exit.",
        ),
    ] = None,
) -> None:
    pass


app.add_typer(setup_app)
app.add_typer(proxy_app)
app.add_typer(ping_app)
