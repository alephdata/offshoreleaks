import io
import click
import dataset
from csv import DictReader
from zipfile import ZipFile
from normality import slugify, stringify
from dataset.chunked import ChunkedInsert


def load_table(db, name, fh):
    _, section, _ = name.rsplit(".", 2)
    table = db[section]
    table.drop()
    print("Loading %s -> %s..." % (name, table.name))
    fh = io.TextIOWrapper(fh)
    reader = DictReader(fh, delimiter=",", quotechar='"')
    chunk = ChunkedInsert(table)
    for row in reader:
        row = {slugify(k, sep="_"): stringify(v) for (k, v) in row.items()}
        chunk.insert(row)
    chunk.flush()


@click.command()
@click.argument("zip_file", type=click.File(mode="rb"))
@click.argument("db_path", type=click.Path(writable=True, resolve_path=True))
def make_db(zip_file, db_path):
    db = dataset.connect("sqlite:///%s" % db_path)
    with ZipFile(zip_file, "r") as zip:
        for name in zip.namelist():
            with zip.open(name) as fh:
                load_table(db, name, fh)


if __name__ == "__main__":
    make_db()
