import typer
from pathlib import Path

from faersdb.config import settings
from faersdb.db import get_conn
from faersdb.manifest import discover_quarters
from faersdb.detect import discover_files

app = typer.Typer()


@app.command()
def scan():
    quarters = discover_quarters(Path(settings.data_root))
    for q in quarters:
        typer.echo(q)


@app.command()
def init_db():
    sql_path = Path("sql/001_init.sql")
    sql_text = sql_path.read_text()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    typer.echo("Database initialized.")


@app.command()
def load_manifest():
    root = Path(settings.data_root)
    quarters = discover_quarters(root)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl.load_batch (root_path, status)
                values (%s, %s)
                returning load_batch_id
                """,
                (str(root), "running"),
            )
            load_batch_id = cur.fetchone()[0]

            inserted = 0

            for q in quarters:
                folder = Path(q["folder_path"])
                files = discover_files(folder)

                for table_kind, file_path in files:
                    header_line = None
                    try:
                        with open(file_path, "r", encoding="latin1", errors="ignore") as f:
                            header_line = f.readline().strip()
                    except Exception:
                        header_line = None

                    cur.execute(
                        """
                        insert into etl.source_file (
                            load_batch_id,
                            source_quarter,
                            source_year,
                            source_qtr,
                            source_system,
                            schema_era,
                            folder_name,
                            table_kind,
                            file_path,
                            file_name,
                            file_size_bytes,
                            header_line
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict do nothing
                        """,
                        (
                            load_batch_id,
                            q["source_quarter"],
                            q["source_year"],
                            q["source_qtr"],
                            q["source_system"],
                            q["schema_era"],
                            q["folder_name"],
                            table_kind,
                            str(file_path),
                            file_path.name,
                            file_path.stat().st_size,
                            header_line,
                        ),
                    )
                    inserted += 1

            cur.execute(
                """
                update etl.load_batch
                set status = %s, finished_at = now()
                where load_batch_id = %s
                """,
                ("finished", load_batch_id),
            )

        conn.commit()

    typer.echo(f"Manifest loaded. load_batch_id={load_batch_id}, files_inserted={inserted}")


if __name__ == "__main__":
    app()