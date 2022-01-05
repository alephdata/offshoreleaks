import io
import yaml
import click
import logging
from typing import Dict
from pprint import pprint
from csv import DictReader
from functools import cache
from zipfile import ZipFile
from datetime import datetime
from normality import stringify, slugify
from datapatch import get_lookups

from followthemoney import model
from followthemoney.types import registry
from followthemoney.proxy import EntityProxy
from followthemoney.cli.util import write_object

log = logging.getLogger("oldbftm")
ENTITIES: Dict[str, EntityProxy] = {}
DATE_FORMATS = [
    "%d-%b-%Y",
    "%b %d, %Y",
    "%Y-%m-%d",
    "%Y",
    "%d/%m/%Y",
    "%d.%m.%Y",
    "%d/%m/%y",
]


@cache
def load_lookups():
    with open("oldb.yml", "r", encoding="utf-8") as fh:
        data = yaml.load(fh, Loader=yaml.SafeLoader)
        return get_lookups(data)


@cache
def lookup(section, value):
    result = load_lookups()[section].match(value)
    if result is None:
        log.error(f"[{section}] missing value: {value}")
    return result


def make_entity_id(id):
    if id is None:
        return None
    return f"icijol-{id}"


@cache
def parse_date(text):
    if text is None:
        return None
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date().isoformat
        except ValueError:
            pass
    res = lookup("dates", text)
    if res is not None:
        return res.values
    # log.error("Unparseable date: %s", text)


@cache
def parse_countries(text):
    if text is None:
        return None
    if ";" in text:
        return [parse_countries(t) for t in text.split(";")]
    code = registry.country.clean_text(text)
    if code is None:
        result = lookup("countries", text)
        if result is not None:
            return [parse_countries(v) for v in result.values]
    return code
    # return text.split(",")


def audit_row(row):
    row = {k: v for (k, v) in row.items() if v is not None}
    if len(row):
        log.warning("Data not used: %r", row)


def emit_entity(proxy: EntityProxy):
    assert proxy.id is not None, proxy
    if proxy.id in ENTITIES:
        proxy = ENTITIES[proxy.id].merge(proxy)
    ENTITIES[proxy.id] = proxy


def dump_entities(out_file: click.File):
    log.info("Dumping %d entities to: %s", len(ENTITIES), out_file.name)
    for entity in ENTITIES.values():
        assert not entity.schema.abstract, entity
        write_object(out_file, entity)


def read_rows(zip, file_name):
    with zip.open(file_name) as zfh:
        fh = io.TextIOWrapper(zfh)
        reader = DictReader(fh, delimiter=",", quotechar='"')
        for row in reader:
            yield {k: stringify(v) for (k, v) in row.items()}


def make_row_entity(row, schema):
    id = row.pop("id", row.pop("_id", None))
    proxy = model.make_entity(schema)
    proxy.id = make_entity_id(id)
    if proxy.id is None:
        log.error("No ID: %r", row)
        return
    name = row.pop("name", None)
    proxy.add("name", name)
    former_name = row.pop("former_name", None)
    if name != former_name:
        proxy.add("previousName", former_name)
    original_name = row.pop("original_name", None)
    if original_name != name:
        proxy.add("previousName", original_name)
    proxy.add("icijId", row.pop("node_id", None))
    proxy.add("legalForm", row.pop("company_type", None))
    proxy.add("legalForm", row.pop("type", None))
    date = parse_date(row.pop("incorporation_date", None))
    proxy.add("incorporationDate", date)
    date = parse_date(row.pop("inactivation_date", None))
    proxy.add("dissolutionDate", date)
    date = parse_date(row.pop("struck_off_date", None))
    proxy.add("dissolutionDate", date)

    closed_date = parse_date(row.pop("closed_date", None))
    if proxy.has("dissolutionDate"):
        log.warning("Company has both dissolution date and closed date: %r", proxy)
    else:
        proxy.add("dissolutionDate", closed_date)

    dorm_date = parse_date(row.pop("dorm_date", None))
    if proxy.has("dissolutionDate"):
        log.warning("Company has both dissolution date and dorm date: %r", proxy)
    else:
        proxy.add("dissolutionDate", dorm_date)

    proxy.add("status", row.pop("status", None))
    proxy.add("publisher", row.pop("sourceID", None))
    proxy.add("notes", row.pop("valid_until", None))
    proxy.add("notes", row.pop("note", None))

    row.pop("jurisdiction", None)
    # countries = parse_countries()
    # proxy.add("jurisdiction", countries)
    countries = parse_countries(row.pop("jurisdiction_description", None))
    proxy.add("jurisdiction", countries)
    proxy.add("address", row.pop("address", None))

    countries = parse_countries(row.pop("country_codes", None))
    proxy.add("country", countries)

    countries = parse_countries(row.pop("countries", None))
    proxy.add("country", countries)
    proxy.add("program", row.pop("service_provider", None))

    if schema == "Company":
        proxy.add("ibcRuc", row.pop("ibcRUC", None))

    row.pop("internal_id", None)
    audit_row(row)
    emit_entity(proxy)


def make_row_address(row):
    id = row.pop("id", row.pop("_id", None))
    proxy = model.make_entity("Address")
    proxy.id = make_entity_id(id)
    proxy.add("full", row.pop("address", None))

    name = row.pop("name", None)
    proxy.add("full", name)
    # if name is not None:
    #     log.info("Name [%s] => [%s]", proxy.first("full"), name)

    row.pop("country_codes", None)
    countries = parse_countries(row.pop("countries"))
    proxy.add("country", countries)
    proxy.add("summary", row.pop("valid_until", None))
    proxy.add("remarks", row.pop("note", None))
    proxy.add("publisher", row.pop("sourceID", None))
    proxy.add("recordId", row.pop("node_id"))

    audit_row(row)
    emit_entity(proxy)


def make_row_relationship(row):
    _type = row.pop("_type")
    _start = row.pop("_start")
    start = make_entity_id(_start)
    _end = row.pop("_end")
    link = row.pop("link", None)
    source_id = row.pop("sourceID", None)
    end = make_entity_id(_end)
    res = lookup("relationships", _type)
    if res is None:
        return

    if res.prop is not None:
        entity = model.make_entity("Thing")
        entity.id = start
        entity.add(res.prop, end)
        entity.add("publisher", source_id)
        # print(type_, res.prop, ENTITIES.get(start), ENTITIES.get(end))

    if res.schema is not None:
        start_date = parse_date(row.pop("start_date"))
        end_date = parse_date(row.pop("end_date"))

        rel = model.make_entity(res.schema)
        rel_id = slugify(f"{start}-{end}-{_type}")
        rel.id = make_entity_id(rel_id)
        rel.add("startDate", start_date)
        rel.add("endDate", end_date)
        rel.add(res.status, row.pop("status"))
        rel.add(res.link, link)
        rel.add("publisher", source_id)
        rel.add(res.start, start)
        rel.add(res.end, end)
        emit_entity(rel)

        # this turns legalentity into organization in some cases
        start_ent = model.make_entity(rel.schema.get(res.start).range)
        start_ent.id = start
        emit_entity(start_ent)

        end_ent = model.make_entity(rel.schema.get(res.end).range)
        end_ent.id = end
        emit_entity(end_ent)

    audit_row(row)


@click.command()
@click.argument("zip_file", type=click.File(mode="rb"))
@click.argument("out_file", type=click.File(mode="w"))
def make_db(zip_file, out_file):
    logging.basicConfig(level=logging.INFO)
    with ZipFile(zip_file, "r") as zip:
        log.info("Loading: nodes-entities.csv...")
        for row in read_rows(zip, "nodes-entities.csv"):
            make_row_entity(row, "Company")

        log.info("Loading: nodes-officers.csv...")
        for row in read_rows(zip, "nodes-officers.csv"):
            make_row_entity(row, "LegalEntity")

        log.info("Loading: nodes-intermediaries.csv...")
        for row in read_rows(zip, "nodes-intermediaries.csv"):
            make_row_entity(row, "LegalEntity")

        log.info("Loading: nodes-others.csv...")
        for row in read_rows(zip, "nodes-others.csv"):
            make_row_entity(row, "LegalEntity")

        log.info("Loading: nodes-addresses.csv...")
        for row in read_rows(zip, "nodes-addresses.csv"):
            make_row_address(row)

        log.info("Loading: relationships.csv...")
        for row in read_rows(zip, "relationships.csv"):
            make_row_relationship(row)

    dump_entities(out_file)


if __name__ == "__main__":
    make_db()
