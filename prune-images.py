#!/usr/bin/env python3
# vim: ts=4 et:

# NOTE: this is an experimental work-in-progress

# Ensure we're using the Python virtual env with our installed dependencies
import os
import sys
import textwrap

NOTE = textwrap.dedent("""
    Experimental: Given an image cache YAML file, figure out what needs to be pruned.
    """)

sys.pycache_prefix = 'work/__pycache__'

if not os.path.exists('work'):
    print('FATAL: Work directory does not exist.', file=sys.stderr)
    print(NOTE, file=sys.stderr)
    exit(1)

# Re-execute using the right virtual environment, if necessary.
venv_args = [os.path.join('work', 'bin', 'python3')] + sys.argv
if os.path.join(os.getcwd(), venv_args[0]) != sys.executable:
    print("Re-executing with work environment's Python...\n", file=sys.stderr)
    os.execv(venv_args[0], venv_args)

# We're now in the right Python environment

import argparse
import logging
import re
import time
from collections import defaultdict
from ruamel.yaml import YAML
from pathlib import Path

import clouds


### Constants & Variables

ACTIONS = ['list', 'prune']
CLOUDS = ['aws']
SELECTIONS = ['keep-last', 'unused', 'ALL']
LOGFORMAT = '%(asctime)s - %(levelname)s - %(message)s'

RE_ALPINE = re.compile(r'^alpine-')
RE_RELEASE = re.compile(r'-(edge|[\d\.]+)-')
RE_REVISION = re.compile(r'-r?(\d+)$')
RE_STUFF = re.compile(r'(edge|[\d+\.]+)-(.+)-r?(\d+)$')

### Functions

# allows us to set values deep within an object that might not be fully defined
def dictfactory():
    return defaultdict(dictfactory)


# undo dictfactory() objects to normal objects
def undictfactory(o):
    if isinstance(o, defaultdict):
        o = {k: undictfactory(v) for k, v in o.items()}
    return o


### Command Line & Logging

parser = argparse.ArgumentParser(description=NOTE)
parser.add_argument('--debug', action='store_true', help='enable debug output')
parser.add_argument('--really', action='store_true', help='really prune images')
parser.add_argument('--cloud', choices=CLOUDS, required=True, help='cloud provider')
parser.add_argument('--region', help='specific region, instead of all regions')
# what to prune...
parser.add_argument('--private', action='store_true')
parser.add_argument('--edge-eol', action='store_true')
parser.add_argument('--rc', action='store_true')
parser.add_argument('--eol-unused-not-latest', action='store_true')
parser.add_argument('--eol-not-latest', action='store_true')
parser.add_argument('--unused-not-latest', action='store_true')
parser.add_argument(
    '--use-broker', action='store_true',
    help='use the identity broker to get credentials')
parser.add_argument('cache_file')
args = parser.parse_args()

log = logging.getLogger()
log.setLevel(logging.DEBUG if args.debug else logging.INFO)
console = logging.StreamHandler()
logfmt = logging.Formatter(LOGFORMAT, datefmt='%FT%TZ')
logfmt.converter = time.gmtime
console.setFormatter(logfmt)
log.addHandler(console)
log.debug(args)

# set up credential provider, if we're going to use it
if args.use_broker:
    clouds.set_credential_provider(debug=args.debug)

# what region(s)?
regions = clouds.ADAPTERS[args.cloud].regions
if args.region:
    if args.region not in regions:
        log.error('invalid region: %s', args.region)
        exit(1)
    else:
        regions = [args.region]

filters = {
    'Owners': ['self'],
    'Filters': [
        {'Name': 'state', 'Values': ['available']},
    ]
}

initial = dictfactory()
variants = dictfactory()
removes = dictfactory()
summary = dictfactory()
latest = {}
now = time.gmtime()

# load cache
yaml = YAML()
log.info(f'loading image cache from {args.cache_file}')
cache = yaml.load(Path(args.cache_file))
log.info(f'loaded image cache')


for region in sorted(regions):
    latest = cache[region]['latest']
    images = cache[region]['images']
    log.info(f'--- {region} : {len(images)} ---')

    for id, image in images.items():
        name = image['name']

        if args.private and image['private']:
            log.info(f"{region}\tPRIVATE\t{name}")
            removes[region][id] = image
            summary[region]['PRIVATE'][id] = name
            continue

        if args.edge_eol and image['version'] == 'edge' and image['eol']:
            log.info(f"{region}\tEDGE-EOL\t{name}")
            removes[region][id] = image
            summary[region]['EDGE-EOL'][id] = name
            continue

        if args.rc and image['rc']:
            log.info(f"{region}\tRC\t{name}")
            removes[region][id] = image
            summary[region]['RC'][id] = name
            continue

        unused = image['launched'] == 'Never'
        release_key = image['release_key']
        variant_key = image['variant_key']
        if variant_key not in latest:
            log.warning(f"variant key '{variant_key}' not in latest, skipping.")
            summary[region]['__WTF__'][id] = name
            continue

        latest_release_key = latest[variant_key]['release_key']
        not_latest = release_key != latest_release_key

        if args.eol_unused_not_latest and image['eol'] and unused and not_latest:
            log.info(f"{region}\tEOL-UNUSED-NOT-LATEST\t{name}")
            removes[region][id] = image
            summary[region]['EOL-UNUSED-NOT-LATEST'][id] = name
            continue

        if args.eol_not_latest and image['eol'] and not_latest:
            log.info(f"{region}\tEOL-NOT-LATEST\t{name}")
            removes[region][id] = image
            summary[region]['EOL-NOT-LATEST'][id] = name
            continue

        if args.unused_not_latest and unused and not_latest:
            log.info(f"{region}\tUNUSED-NOT-LATEST\t{name}")
            removes[region][id] = image
            summary[region]['UNUSED-NOT-LATEST'][id] = name
            continue

        log.debug(f"{region}\t__KEPT__\t{name}")
        summary[region]['__KEPT__'][id] = name

totals = {}
log.info('SUMMARY')
for region, reasons in sorted(summary.items()):
    log.info(f"\t{region}")
    for reason, images in sorted(reasons.items()):
        count = len(images)
        log.info(f"\t\t{count}\t{reason}")
        if reason not in totals:
            totals[reason] = 0

        totals[reason] += count

log.info('TOTALS')
for reason, count in sorted(totals.items()):
    log.info(f"\t{count}\t{reason}")

if args.really:
    log.warning('Please confirm you wish to actually prune these images...')
    r = input("(yes/NO): ")
    print()
    if r.lower() != 'yes':
        args.really = False

if not args.really:
    log.warning("Not really pruning any images.")
    exit(0)

# do the pruning...

for region, images in sorted(removes.items()):
    ec2r = clouds.ADAPTERS[args.cloud].session(region).resource('ec2')
    for id, image in images.items():
        name = image['name']
        snapshot_id = image['snapshot_id']
        try:
            log.info(f'Deregistering: {region}/{id}: {name}')
            ec2r.Image(id).deregister()
            log.info(f"Deleting: {region}/{snapshot_id}: {name}")
            ec2r.Snapshot(snapshot_id).delete()

        except Exception as e:
            log.warning(f"Failed: {e}")
            pass

log.info('DONE')
