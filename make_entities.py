import click
import dataset
from datetime import datetime
import ftmstore
from followthemoney import model
from followthemoney.cli.util import write_object
from ftmstore import store
from sqlalchemy.exc import OperationalError


REPRESENTATION_INTERVALS = [
    "underlying",
    "intermediary_of",
    "connected_to"
]

IGNORE_INTERVALLS = [
    "same_name_as",
    "similar",
    "same_as",
    "same_id_as",
    "same_address_as",
    "same_company_as",
    "similar_company_as",
    "same_intermediary_as",
    "probably_same_officer_as"
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
    "sec",
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


def make_node_entity(id, schema="LegalEntity"):
    proxy = model.make_entity(schema)
    proxy.id = id
    return(proxy)

def parse_date(text):
    if text is None:
        return("")
    try:
        return(datetime.strptime(text, "%d-%m-%Y"))
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
    if not link:
        print(type_, link)
        link = type_
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
        source = make_node_entity(source_id)
        target_id = interv.pop("end", None)
        target = make_node_entity(target_id)
        type_ = interv.pop("type", None)
        link = interv.pop("link", None)

        if type_ in IGNORE_INTERVALLS:
            continue

        if not source_id or not target_id:
            continue

        schema = interv_schema(type_, link)
        proxy = model.make_entity(schema)
        proxy.make_id(source_id, target_id, type_, link)
        proxy.add("startDate", parse_date(interv.pop("start_date", None)))
        proxy.add("endDate", parse_date(interv.pop("end_date", None)))
        proxy.add("summary", interv.pop("valid_until", None))
        proxy.add("role", link)
        if link is None:
            proxy.add("role", type_)
        
        if schema == "Representation":
            proxy.add('agent', source)
            proxy.add('client', target)
        elif schema == "Directorship":
            proxy.add('director', source)
            proxy.add('organization', target)
        elif schema == "Ownership":
            proxy.add('owner', source)
            proxy.add('asset', target)
        else:
            proxy.add('subject', source)
            proxy.add('object', target)

        writer.put(proxy)

        if i % 10000 == 0:
            print("interval: %s" % i)


def make_row_entity(row, schema):
    id = row.pop("id", None)
    proxy = make_node_entity(id, schema)
    proxy.add("name", row.pop("name", None))
    proxy.add("previousName", row.pop("former_name", None))
    proxy.add("weakAlias", row.pop("original_name", None))
    proxy.add("icijId", row.pop("node_id", None))
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
    row.pop("dorm_date", None)

    if len(row):
        print('info not used:', row)

    return(proxy)


def write_nodes(writer, table, schema="LegalEntity"):
    for i, row in enumerate(table, 1):
        id = row.get("id")
        proxy = make_row_entity(row, schema)
        if i % 10000 == 0:
            print("%s: %s" % (table.name, i))
        writer.put(proxy, fragment=id)
    writer.flush()

def write_addresses(writer, table):
    schema = "Address"
    for i, row in enumerate(table, 1):
        id = row.get("id")
        proxy = make_node_entity(id, schema)
        proxy.add("full", row.get("address"))
        countries = parse_countries(row.get("country_codes"))
        proxy.add("country", countries)
        proxy.add("summary", row.pop("valid_until", None))
        proxy.add("publisher", row.pop("sourceid", None))
        if i % 10000 == 0:
            print("%s: %s" % (table.name, i))
        writer.put(proxy, fragment=id)
    writer.flush()


@click.command()
@click.argument("db_path", type=click.Path(exists=True))
@click.argument("outfile", type=click.File(mode="w"))
def make_entities(db_path, outfile):
    db = dataset.connect("sqlite:///%s" % db_path)
    store = ftmstore.get_dataset("temp", database_uri="sqlite://")
    writer = store.bulk()
    write_nodes(writer, db.load_table("nodes-entities"), "Company")
    write_nodes(writer, db.load_table("nodes-intermediaries"))
    write_nodes(writer, db.load_table("nodes-officers"))
    write_nodes(writer, db.load_table("nodes-others"))
    write_addresses(writer, db.load_table("nodes-addresses"))
    write_intervalls(writer, db)
    for entity in store.iterate():
        write_object(outfile, entity)
if __name__ == "__main__":
    make_entities()
