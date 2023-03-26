#!/usr/bin/env python3
# vim: ts=4 et:

# TODO: perhaps integrate into "./build release"

# Ensure we're using the Python virtual env with our installed dependencies
import os
import sys
import textwrap

NOTE = textwrap.dedent("""
    This script's output provides a mustache-ready datasource to alpine-mksite
    (https://gitlab.alpinelinux.org/alpine/infra/alpine-mksite) and should be
    run after the main 'build' script has published ALL images.
    STDOUT from this script should be saved as 'cloud/releases.yaml' in the
    above alpine-mksite repo.
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

from collections import defaultdict
from ruamel.yaml import YAML

import clouds
from image_config_manager import ImageConfigManager


### Constants & Variables

LOGFORMAT = '%(name)s - %(levelname)s - %(message)s'


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
parser.add_argument(
    '--use-broker', action='store_true',
    help='use the identity broker to get credentials')
parser.add_argument('--debug', action='store_true', help='enable debug output')
args = parser.parse_args()

log = logging.getLogger('gen_mksite_releases')
log.setLevel(logging.DEBUG if args.debug else logging.INFO)
console = logging.StreamHandler(sys.stderr)
console.setFormatter(logging.Formatter(LOGFORMAT))
log.addHandler(console)
log.debug(args)

# set up credential provider, if we're going to use it
if args.use_broker:
    clouds.set_credential_provider()

# load build configs
configs = ImageConfigManager(
    conf_path='work/configs/images.conf',
    yaml_path='work/images.yaml',
    log='gen_mksite_releases'
)
# make sure images.yaml is up-to-date with reality
configs.refresh_state('final')

yaml = YAML()

filters = dictfactory()
versions = dictfactory()
data = {}

log.info('Transforming image data')
for i_key, i_cfg in configs.get().items():
    if not i_cfg.published:
        continue

    version = i_cfg.version
    if version == 'edge':
        continue

    image_name = i_cfg.image_name
    release = i_cfg.release
    arch = i_cfg.arch
    firmware = i_cfg.firmware
    bootstrap = i_cfg.bootstrap
    cloud = i_cfg.cloud

    if cloud not in filters['clouds']:
        filters['clouds'][cloud] = {
            'cloud': cloud,
            'cloud_name': i_cfg.cloud_name,
        }

    filters['regions'] = {}

    if arch not in filters['archs']:
        filters['archs'][arch] = {
            'arch': arch,
            'arch_name': i_cfg.arch_name,
        }

    if firmware not in filters['firmwares']:
        filters['firmwares'][firmware] = {
            'firmware': firmware,
            'firmware_name': i_cfg.firmware_name,
        }

    if bootstrap not in filters['bootstraps']:
        filters['bootstraps'][bootstrap] = {
            'bootstrap': bootstrap,
            'bootstrap_name': i_cfg.bootstrap_name,
        }

    if i_cfg.artifacts:
        for region, image_id in {r: i_cfg.artifacts[r] for r in sorted(i_cfg.artifacts)}.items():
            if region not in filters['regions']:
                filters['regions'][region] = {
                    'region': region,
                    'clouds': [cloud],
                }

            if cloud not in filters['regions'][region]['clouds']:
                filters['regions'][region]['clouds'].append(cloud)

            versions[version] |= {
                'version': version,
                'release': release,
                'end_of_life': i_cfg.end_of_life,
            }
            versions[version]['images'][image_name] |= {
                'image_name': image_name,
                'arch': arch,
                'firmware': firmware,
                'bootstrap': bootstrap,
                'published': i_cfg.published.split('T')[0],     # just the date
            }
            versions[version]['images'][image_name]['downloads'][cloud] |= {
                'cloud': cloud,
                'image_format': i_cfg.image_format,
                'image_url':  i_cfg.download_url + '/' + (i_cfg.image_name)
            }
            versions[version]['images'][image_name]['regions'][region] |= {
                'cloud': cloud,
                'region': region,
                'region_url': i_cfg.region_url(region, image_id),
                'launch_url': i_cfg.launch_url(region, image_id),
            }

log.info('Making data mustache-compatible')

# convert filters to mustache-compatible format
data['filters'] = {}
for f in ['clouds', 'regions', 'archs', 'firmwares', 'bootstraps']:
    data['filters'][f] = [
        filters[f][k] for k in filters[f]   # order as they appear in work/images.yaml
    ]

for r in data['filters']['regions']:
    c = r.pop('clouds')
    r['clouds'] = [{'cloud': v} for v in c]

# convert versions to mustache-compatible format
data['versions'] = []
versions = undictfactory(versions)
for version in sorted(versions, reverse=True, key=lambda s: [int(u) for u in s.split('.')]):
    images = versions[version].pop('images')
    i = []
    for image_name in images:   # order as they appear in work/images.yaml
        downloads = images[image_name].pop('downloads')
        d = []
        for download in downloads:
            d.append(downloads[download])

        images[image_name]['downloads'] = d

        regions = images[image_name].pop('regions')
        r = []
        for region in sorted(regions):
            r.append(regions[region])

        images[image_name]['regions'] = r
        i.append(images[image_name])

    versions[version]['images'] = i
    data['versions'].append(versions[version])

log.info('Dumping YAML')
yaml.dump(data, sys.stdout)
log.info('Done')
