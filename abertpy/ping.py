import sys

from pydantic_typer import Typer

app = Typer()


@app.command()
def ping():
    print("vk496")
    sys.exit(18)
