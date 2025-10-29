from pydantic_typer import Typer

from abertpy.ping import app as ping_app
from abertpy.proxy import app as proxy_app
from abertpy.setup import app as setup_app

app = Typer()


app.add_typer(setup_app)
app.add_typer(proxy_app)
app.add_typer(ping_app)
