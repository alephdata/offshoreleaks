import click
import dataset


@click.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("outfile", type=click.File(mode="w"))
def make_entities(db_path, outfile):
    db = dataset.connect("sqlite:///%s" % db_path)
    outfile.write("hello")


if __name__ == "__main__":
    make_entities()
