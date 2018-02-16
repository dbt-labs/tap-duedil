#!/usr/bin/env python3
import os
import sys
import json
import singer
import argparse
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context

REQUIRED_CONFIG_KEYS = ["api_key"]
LOGGER = singer.get_logger()


def load_json(path):
    with open(path) as fil:
        return json.load(fil)


def parse_args(required_config_keys):
    parser = argparse.ArgumentParser()

    base_subparser = argparse.ArgumentParser(add_help=False)
    subs = parser.add_subparsers()

    discover_sub = subs.add_parser('discover', parents=[base_subparser])
    discover_sub.set_defaults(which='discover')

    sync_sub = subs.add_parser('sync', parents=[base_subparser])
    sync_sub.set_defaults(which='sync')
    sync_sub.add_argument(
        '-s', '--state',
        help='State file')

    stream_names = [s for s in streams_.all_stream_ids if s != 'company_query']
    stream_names_list = " ".join(stream_names)
    sync_sub.add_argument(
        '--streams',
        nargs='+',
        default=stream_names,
        help='Endpoints to sync. Default = \n{}'.format(stream_names_list))

    query_sub = subs.add_parser('query', parents=[base_subparser])
    query_sub.set_defaults(which='query')
    query_sub.add_argument(
        '-q', '--query',
        required=True,
        help='Query to use to find companies')

    for sub in [query_sub, sync_sub]:
        sub.add_argument(
            '-c', '--config',
            help='Config file',
            required=True)

        sub.add_argument(
            '-p', '--properties',
            required=True,
            help='Catalog file')

        sub.add_argument(
            '--companies',
            required=True,
            help='File containing a cache of company IDs, one per line')


    args = parser.parse_args()
    if not hasattr(args, 'which'):
        LOGGER.error("Usage: tap-duedil [query|sync] -h")
        exit(1)

    if args.which == 'discover':
        args.config = {}
        args.state = {}
        args.properties = {}
        return args

    if args.config:
        args.config = load_json(args.config)

    if hasattr(args, 'state') and args.state:
        args.state = load_json(args.state)
    else:
        args.state = {}

    if args.properties:
        args.properties = load_json(args.properties)

    if hasattr(args, 'query') and args.query:
        args.query = load_json(args.query)

    check_config(args.config, required_config_keys)
    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def make_null_tolerant(schema, depth=0):
    if not isinstance(schema, dict):
        return schema

    tolerant_schema = {}
    for key in schema.keys():
        value = schema[key]
        if depth > 0 and key == 'type' and isinstance(value, str) and value != 'null':
            value = [schema[key], "null"]
        elif isinstance(value, dict):
            value = make_null_tolerant(value, depth+1)

        tolerant_schema[key] = value
    return tolerant_schema


def load_schema(ctx, tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(ctx, sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)

    null_tolerant_schema = make_null_tolerant(schema)
    return null_tolerant_schema


def load_and_write_schema(ctx, stream):
    singer.write_schema(
        stream.tap_stream_id,
        load_schema(ctx, stream.tap_stream_id),
        stream.pk_fields,
    )

def discover(ctx):
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema = Schema.from_dict(load_schema(ctx, stream.tap_stream_id),
                                  inclusion="automatic")
        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
        ))
    return catalog


def sync(ctx, selected_streams, company_filename):
    companies = []
    with open(company_filename) as fh:
        for line in fh.readlines():
            line = line.strip()
            if len(line) == 0:
                continue
            else:
                data = json.loads(line)
                companies.append(data)

    ctx.cache['companies'] = companies
    streams = [s for s in streams_.all_streams if s.tap_stream_id in selected_streams]

    # chunk these companies up into groups
    CHUNK_SIZE = 50
    company_chunks = [companies[x:x+CHUNK_SIZE] for x in range(0, len(companies), CHUNK_SIZE)]

    start_index = ctx.state.get('company_index', 0)
    start_chunk = int(start_index / CHUNK_SIZE)
    company_chunks = company_chunks[start_chunk:]

    company_index = start_index
    for i, company_chunk in enumerate(company_chunks):
        ctx.state["company_index"] = company_index
        for stream in streams:
            load_and_write_schema(ctx, stream)
            stream.sync(ctx, company_chunk, i, len(company_chunks))

        company_index += len(company_chunk)
        ctx.write_state()
    ctx.write_state()


def fetch_companies(ctx, query, company_filename):
    company_query = streams_.company_query
    load_and_write_schema(ctx, company_query)
    streams_.company_query.fetch_into_cache(ctx, query)
    streams_.company_query.sync(ctx)

    with open(company_filename, 'w') as fh:
        for company in ctx.cache['companies']:
            data = {
                    "companyId": company['companyId'],
                    "countryCode": company['countryCode']
            }
            line = json.dumps(data)
            fh.write('{}\n'.format(line))


def main_impl():
    args = parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)

    if args.which == 'discover':
        discover(ctx).dump()
        print()
    elif args.which == 'query':
        ctx.catalog = Catalog.from_dict(args.properties)
        fetch_companies(ctx, args.query, args.companies)
    else:
        ctx.catalog = Catalog.from_dict(args.properties)
        sync(ctx, args.streams, args.companies)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise

if __name__ == "__main__":
    main()
