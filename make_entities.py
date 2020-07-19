import click
import dataset
from datetime import datetime
from ftmstore import Dataset
from followthemoney import model
from followthemoney.cli.util import write_object
from sqlalchemy.exc import OperationalError

IGNORE_EDGE_TYPES = ["registered_address", "same_name_as", "same_address_as"]
SAME_AS = ["same_intermediary_as", "same_company_as", "same_as"]
OWNER_EDGES = [
    "owner",
    "beneficial",
    "shareholder",
    "beneficiary",
    "founder",
    "settlor",
]
DIRECTOR_EDGES = [
    "director",
    "secretary",
    "president",
    "member",
    "signatory",
    "protector",
    "executive",
    "chairman",
    "partner",
    "leader",
    "treasurer",
    "administrator",
    "trustee",
    "nominated person",
    "chief financial officer",
]
MEMBER_EDGES = [
    "manager",
    "business man",
    "representative",
    "dean",
    "financial officer",
]


def make_node_entity(node_id, schema="LegalEntity"):
    proxy = model.make_entity(schema)
    proxy.id = "icij-%s" % node_id
    return proxy


def parse_date(text):
    if text is None:
        return
    try:
        return datetime.strptime(text, "%d-%b-%Y")
    except ValueError:
        return text


def parse_countries(text):
    if text is not None:
        return text.split(";")


def edge_schema(type_, link):
    link = link or type_
    link = link.lower()
    for term in OWNER_EDGES:
        if term in link:
            return "Ownership"
    for term in DIRECTOR_EDGES:
        if term in link:
            return "Directorship"
    for term in MEMBER_EDGES:
        if term in link:
            return "Membership"
    return "UnknownLink"


def write_edges(writer, db):
    for i, edge in enumerate(db["edges"], 1):
        source_id = edge.pop("start_id", None)
        source_id = edge.pop("node_1", source_id)
        source = make_node_entity(source_id)
        target_id = edge.pop("end_id", None)
        target_id = edge.pop("node_2", target_id)
        target = make_node_entity(target_id)
        type_ = edge.pop("rel_type", None)
        type_ = edge.pop("type", type_)
        link = edge.pop("link", None)
        if type_ in IGNORE_EDGE_TYPES:
            continue
        if type_ in SAME_AS:
            source.add("sameAs", target)
            writer.put(source, fragment=target.id)
            target.add("sameAs", source)
            writer.put(target, fragment=source.id)
            continue
        schema = edge_schema(type_, link)
        # print(type_, link, schema)
        proxy = model.make_entity(schema)
        proxy.make_id(source_id, target_id, type_, link)
        proxy.add("startDate", parse_date(edge.pop("start_date", None)))
        proxy.add("endDate", parse_date(edge.pop("end_date", None)))
        proxy.add("summary", edge.pop("valid_until", None))
        proxy.add(proxy.schema.source_prop, source)
        proxy.add(proxy.schema.target_prop, target)
        proxy.add("role", link)
        if link is None:
            proxy.add("role", type_)
        writer.put(proxy)

        if i % 10000 == 0:
            print("edges: %s" % i)


def make_row_entity(row, schema):
    node_id = row.pop("node_id", None)
    proxy = make_node_entity(node_id, schema)
    proxy.add("name", row.pop("name", None))
    proxy.add("icijId", node_id)
    proxy.add("legalForm", row.pop("company_type", None))
    date = parse_date(row.pop("incorporation_date", None))
    proxy.add("incorporationDate", date)
    date = parse_date(row.pop("inactivation_date", None))
    proxy.add("dissolutionDate", date)
    date = parse_date(row.pop("struck_off_date", None))
    proxy.add("dissolutionDate", date)
    proxy.add("status", row.pop("status", None))
    proxy.add("publisher", row.pop("sourceid", None))
    proxy.add("notes", row.pop("valid_until", None))
    proxy.add("notes", row.pop("note", None))
    countries = parse_countries(row.pop("jurisdiction", None))
    proxy.add("jurisdiction", countries)
    countries = parse_countries(row.pop("jurisdiction_description", None))
    proxy.add("jurisdiction", countries)
    proxy.add("address", row.pop("address", None))
    countries = parse_countries(row.pop("country_codes", None))
    proxy.add("country", countries)
    countries = parse_countries(row.pop("countries", None))
    proxy.add("country", countries)
    proxy.add("registrationNumber", row.pop("ibcruc", None))
    proxy.add("program", row.pop("service_provider", None))

    row.pop("id", None)
    row.pop("type", None)
    row.pop("labels_n", None)
    row.pop("closed_date", None)

    if len(row):
        print(row)

    return proxy


def write_nodes(writer, table, schema="LegalEntity"):
    for i, row in enumerate(table, 1):
        node_id = row.get("node_id")
        proxy = make_row_entity(row, schema)
        if i % 10000 == 0:
            print("%s: %s" % (table.name, i))
        writer.put(proxy, fragment=node_id)
    writer.flush()


def _write_addresses(writer, db, query):
    for i, row in enumerate(db.query(query), 1):
        proxy = make_node_entity(row.get("entity_id"))
        proxy.add("address", row.get("address"))
        countries = parse_countries(row.get("country_codes"))
        proxy.add("country", countries)
        if i % 10000 == 0:
            print("adj address: %s" % i)
        writer.put(proxy, fragment=row.get("edge_id"))
    writer.flush()


def write_addresses(writer, db):
    try:
        query = """
            SELECT e.id AS edge_id, e.start_id AS entity_id,
                a.address AS address, a.country_codes AS country_codes
            FROM edges e JOIN address a
                ON e.end_id = a.node_id;
        """
        _write_addresses(writer, db, query)
    except OperationalError:
        query = """
            SELECT e.id AS edge_id, e.node_1 AS entity_id,
                a.address AS address, a.country_codes AS country_codes
            FROM edges e JOIN address a
                ON e.node_2 = a.node_id;
        """
        _write_addresses(writer, db, query)


@click.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("outfile", type=click.File(mode="w"))
def make_entities(db_path, outfile):
    db = dataset.connect("sqlite:///%s" % db_path)
    store = Dataset("temp", database_uri="sqlite://")
    writer = store.bulk()
    write_edges(writer, db)
    write_addresses(writer, db)
    write_nodes(writer, db["entity"], "Company")
    write_nodes(writer, db["intermediary"])
    write_nodes(writer, db["officer"])

    for entity in store.iterate():
        write_object(outfile, entity)


if __name__ == "__main__":
    make_entities()
