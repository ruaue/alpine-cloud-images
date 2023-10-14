#!/usr/bin/env python3
# vim: ts=4 et:

# NOTE: this is an experimental work-in-progress

# Ensure we're using the Python virtual env with our installed dependencies
import os
import sys
import textwrap

NOTE = textwrap.dedent("""
    Experimental:  Outputs image cache YAML on STDOUT for use with prune-images.py
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

import clouds


### Constants & Variables

CLOUDS = ['aws']
LOGFORMAT = '%(asctime)s - %(levelname)s - %(message)s'

RE_ALPINE = re.compile(r'^alpine-')
RE_RELEASE = re.compile(r'-(edge|[\d\.]+)-')
RE_REVISION = re.compile(r'-r?(\d+)$')
RE_STUFF = re.compile(r'(edge|[\d+\.]+)(?:_rc(\d+))?-(.+)-r?(\d+)$')


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
parser.add_argument('--cloud', choices=CLOUDS, required=True, help='cloud provider')
parser.add_argument('--region', help='specific region, instead of all regions')
parser.add_argument(
    '--use-broker', action='store_true',
    help='use the identity broker to get credentials')
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

data = dictfactory()
now = time.gmtime()

for region in sorted(regions):
    # TODO: make more generic if we need to do this for other clouds someday
    ec2r = clouds.ADAPTERS[args.cloud].session(region).resource('ec2')
    images = sorted(ec2r.images.filter(**filters), key=lambda k: k.creation_date)
    log.info(f'--- {region} : {len(images)} ---')
    version = release = revision = None

    for image in images:
        latest = data[region]['latest']     # shortcut

        # information about the image
        id = image.id
        name = image.name

        # only consider images named /^alpine-/
        if not RE_ALPINE.search(image.name):
            log.warning(f'IGNORING {region}\t{id}\t{name}')
            continue

        # parse image name for more information
        # NOTE: we can't rely on tags, because they may not have been set successfully
        m = RE_STUFF.search(name)
        if not m:
            log.error(f'!PARSE\t{region}\t{id}\t{name}')
            continue

        release = m.group(1)
        rc = m.group(2)
        version = '.'.join(release.split('.')[0:2])
        variant = m.group(3)
        revision = m.group(4)
        variant_key = '-'.join([version, variant])
        release_key = revision if release == 'edge' else '-'.join([release, revision])

        last_launched_attr = image.describe_attribute(Attribute='lastLaunchedTime')['LastLaunchedTime']
        last_launched = last_launched_attr.get('Value', 'Never')

        eol = time.strptime(image.deprecation_time ,'%Y-%m-%dT%H:%M:%S.%fZ') < now

        # keep track of images
        data[region]['images'][id] = {
            'name': name,
            'release': release,
            'version': version,
            'variant': variant,
            'revision': revision,
            'variant_key': variant_key,
            'release_key': release_key,
            'created': image.creation_date,
            'launched': last_launched,
            'deprecated': image.deprecation_time,
            'rc': rc is not None,
            'eol': eol,
            'private': not image.public,
            'snapshot_id': image.block_device_mappings[0]['Ebs']['SnapshotId']
        }

        # keep track of the latest release_key per variant_key
        if variant_key not in latest or (release > latest[variant_key]['release']) or (release == latest[variant_key]['release'] and [revision > latest[variant_key]['revision']]):
            data[region]['latest'][variant_key] = {
                'release': release,
                'revision': revision,
                'release_key': release_key
            }

        log.info(f'{region}\t{not image.public}\t{eol}\t{last_launched.split("T")[0]}\t{name}')

# instantiate YAML
yaml = YAML()
yaml.explicit_start = True

# TODO?  dump out to a file instead of STDOUT?
yaml.dump(undictfactory(data), sys.stdout)

total = 0
for region, rdata in sorted(data.items()):
    count = len(rdata['images'])
    log.info(f'{region} : {count} images')
    total += count

log.info(f'TOTAL : {total} images')
