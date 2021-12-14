import click
import dataset
from datetime import datetime
import ftmstore
# from ftmstore import Dataset
from followthemoney import model
from followthemoney.cli.util import write_object
from sqlalchemy.exc import OperationalError

IGNORE_INTERVAL_TYPES = [
    "registered_address",
    "same_address_as",
    "same_company_as",
    "similar_company_as",
    "same_intermediary_as",
    "probably_same_officer_as",
    "similar",
    "same_name_as", 
    "same_company_as", 
    "same_as",
    "same_id_as"
]

REPRESENTATION_INTERVALS = [
    "underlying",
    "intermediary_of",
    "connected_to"
]

OWNERSHIP = [
    "benef",
    "owner",
    "ubo",
    "investor",
    "shareholder"   
]

DIRECTORSHIP = [
    "director",
    "secret",
    "treas",
    "pres",
    "vice",
    "chair",
    "power",
    "poa",
    "exec",
    "assist",
    "chief",
    "officer"
]


def make_node_entity(node_id, schema="LegalEntity"):
    proxy = model.make_entity(schema)
    proxy.id = "icij-%s" % node_id
    return(proxy)

def parse_date(text):
    if text is None:
        return("")
    try:
        return(datetime.strptime(text, "%d-%b-%Y"))
    except ValueError:
        return(text)

def parse_countries(text):
    if text is not None:
        return(text.split(","))

def define_officer(link):
    link = link.lower()
    if any(x in link for x in OWNERSHIP):
        return("Ownership")
    elif any(x in link for x in DIRECTORSHIP):
        return("Directorship")
    else:
        return("Representation")

def interv_schema(type_, link):
    # link = link or type_
    link = link.lower()
    if "officer" in type_:
        officer = define_officer(link)
        return(officer)
    for term in REPRESENTATION_INTERVALS:
        if term in type_:
            return("Representation")
    return("UnknownLink")

def write_intervalls(writer, db):
    for i, interv in enumerate(db.load_table("relationships"), 1):
        source_id = interv.pop("start", None)
        # source_id = interv.pop("node_1", source_id)
        source = make_node_entity(source_id)
        target_id = interv.pop("end", None)
        # target_id = interv.pop("node_2", target_id)
        target = make_node_entity(target_id)
        # type_ = interv.pop("rel_type", None)
        type_ = interv.pop("type", None)
        link = interv.pop("link", None)
        if type_ in IGNORE_INTERVAL_TYPES:
            continue
        # if type_ in SAME_AS:
        #     source.add("sameAs", target)
        #     writer.put(source, fragment=target.id)
        #     target.add("sameAs", source)
        #     writer.put(target, fragment=source.id)
        #     continue
        schema = interv_schema(type_, link)
        #   print(type_, link, schema)
        proxy = model.make_entity(schema)
        proxy.make_id(source_id, target_id, type_, link)
        proxy.add("startDate", parse_date(interv.pop("start_date", None)))
        proxy.add("endDate", parse_date(interv.pop("end_date", None)))
        proxy.add("summary", interv.pop("valid_until", None))
        proxy.add(proxy.schema.source_prop, source)
        proxy.add(proxy.schema.target_prop, target)
        proxy.add("role", link)
        if link is None:
            proxy.add("role", type_)
        writer.put(proxy)

        if i % 10000 == 0:
            print("interval: %s" % i)


def make_row_entity(row, schema):
    node_id = row.pop("node_id", None)
    proxy = make_node_entity(node_id, schema)
    proxy.add("name", row.pop("name", None))
    proxy.add("previousName", row.pop("former_name", None))
    proxy.add("weakAlias", row.pop("original_name", None))
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
    proxy.add("program", row.pop("service_provider", None))

    if schema == 'Company':
        proxy.add("ibcRuc", row.pop("ibcruc", None))

    row.pop("internal_id", None)
    row.pop("id", None)
    row.pop("dorm_date", None)

    if len(row):
        print('info not used:', row)

    return(proxy)


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
        writer.put(proxy, fragment=row.get("interv_id"))
    writer.flush()


def write_addresses(writer, db):
    try:
        query = """
            SELECT
                e.id AS interv_id,
                start AS entity_id,
                address,
                country_codes
            FROM
                relationships e
                JOIN "nodes-addresses" a ON e.end = a.node_id;
        """
        _write_addresses(writer, db, query)
    except Exception as e:
        print(e)

@click.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("outfile", type=click.File(mode="w"))
def make_entities(db_path, outfile):
    db = dataset.connect("sqlite:///%s" % db_path)
    store = ftmstore.get_dataset("temp", database_uri="sqlite://")
    writer = store.bulk()
    write_intervalls(writer, db)
    write_addresses(writer, db)
    write_nodes(writer, db["node-entities"], "Company")
    write_nodes(writer, db["node-intermediary"])
    write_nodes(writer, db["node-officer"])
    write_nodes(writer, db.load_table("nodes-entities"), "Company")
    write_nodes(writer, db.load_table("nodes-intermediaries"))
    write_nodes(writer, db.load_table("nodes-officers"))

    for entity in store.iterate():
        write_object(outfile, entity)

if __name__ == "__main__":
    make_entities()