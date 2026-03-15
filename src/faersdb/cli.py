import typer
from pathlib import Path
from faersdb.config import settings
from faersdb.manifest import discover_quarters

app = typer.Typer()

@app.command()
def scan():
    quarters = discover_quarters(Path(settings.data_root))
    for q in quarters:
        typer.echo(q)

@app.command()
def load_manifest():
    # insert into etl.load_batch and etl.source_file
    ...

@app.command()
def load_staging(kind: str | None = None):
    # load raw quarter files into staging
    ...

@app.command()
def normalize():
    # build core.case_master and core.case_version, then children
    ...

@app.command()
def refresh_marts():
    # refresh materialized views
    ...

if __name__ == "__main__":
    app()