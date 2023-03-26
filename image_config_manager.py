# vim: ts=4 et:

import itertools
import logging
import pyhocon

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from ruamel.yaml import YAML

from image_config import ImageConfig



class ImageConfigManager():

    def __init__(self, conf_path, yaml_path, log=__name__, alpine=None):
        self.conf_path = Path(conf_path)
        self.yaml_path = Path(yaml_path)
        self.log = logging.getLogger(log)
        self.alpine = alpine

        self.now = datetime.utcnow()
        self._configs = {}

        self.yaml = YAML()
        self.yaml.register_class(ImageConfig)
        self.yaml.explicit_start = True
        # hide !ImageConfig tag from Packer
        self.yaml.representer.org_represent_mapping = self.yaml.representer.represent_mapping
        self.yaml.representer.represent_mapping = self._strip_yaml_tag_type

        # load resolved YAML, if exists
        if self.yaml_path.exists():
            self._load_yaml()
        else:
            self._resolve()

    def get(self, key=None):
        if not key:
            return self._configs

        return self._configs[key]

    # load already-resolved YAML configs, restoring ImageConfig objects
    def _load_yaml(self):
        self.log.info('Loading existing %s', self.yaml_path)
        for key, config in self.yaml.load(self.yaml_path).items():
            self._configs[key] = ImageConfig(key, config, log=self.log, yaml=self.yaml)

    # save resolved configs to YAML
    def _save_yaml(self):
        self.log.info('Saving %s', self.yaml_path)
        self.yaml.dump(self._configs, self.yaml_path)

    # hide !ImageConfig tag from Packer
    def _strip_yaml_tag_type(self, tag, mapping, flow_style=None):
        if tag == '!ImageConfig':
            tag = u'tag:yaml.org,2002:map'

        return self.yaml.representer.org_represent_mapping(tag, mapping, flow_style=flow_style)

    # resolve from HOCON configs
    def _resolve(self):
        self.log.info('Generating configs.yaml in work environment')
        cfg = pyhocon.ConfigFactory.parse_file(self.conf_path)
        # set version releases
        for v, vcfg in cfg.Dimensions.version.items():
            # version keys are quoted to protect dots
            self._set_version_release(v.strip('"'), vcfg)

        dimensions = list(cfg.Dimensions.keys())
        self.log.debug('dimensions: %s', dimensions)

        for dim_keys in (itertools.product(*cfg['Dimensions'].values())):
            config_key = '-'.join(dim_keys).replace('"', '')

            # dict of dimension -> dimension_key
            dim_map = dict(zip(dimensions, dim_keys))

            # replace version with release, and make image_key from that
            release = cfg.Dimensions.version[dim_map['version']].release
            (rel_map := dim_map.copy())['version'] = release
            image_key = '-'.join(rel_map.values())

            image_config = ImageConfig(
                config_key,
                {
                    'image_key': image_key,
                    'release': release
                } | dim_map,
                log=self.log,
                yaml=self.yaml
            )

            # merge in the Default config
            image_config._merge(cfg.Default)
            skip = False
            # merge in each dimension key's configs
            for dim, dim_key in dim_map.items():
                dim_cfg = deepcopy(cfg.Dimensions[dim][dim_key])

                image_config._merge(dim_cfg)

                # now that we're done with ConfigTree/dim_cfg, remove " from dim_keys
                dim_keys = set(k.replace('"', '') for k in dim_keys)

                # WHEN blocks inside WHEN blocks are considered "and" operations
                while (when := image_config._pop('WHEN', None)):
                    for when_keys, when_conf in when.items():
                        # WHEN keys with spaces are considered "or" operations
                        if len(set(when_keys.split(' ')) & dim_keys) > 0:
                            image_config._merge(when_conf)

                exclude = image_config._pop('EXCLUDE', None)
                if exclude and set(exclude) & set(dim_keys):
                    self.log.debug('%s SKIPPED, %s excludes %s', config_key, dim_key, exclude)
                    skip = True
                    break

                if eol := image_config._get('end_of_life', None):
                    if self.now > datetime.fromisoformat(eol):
                        self.log.warning('%s SKIPPED, %s end_of_life %s', config_key, dim_key, eol)
                        skip = True
                        break

            if skip is True:
                continue

            # merge in the Mandatory configs at the end
            image_config._merge(cfg.Mandatory)

            # clean stuff up
            image_config._normalize()
            image_config.qemu['iso_url'] = self.alpine.virt_iso_url(arch=image_config.arch)

            # we've resolved everything, add tags attribute to config
            self._configs[config_key] = image_config

        self._save_yaml()

    # set current version release
    def _set_version_release(self, v, c):
        info = self.alpine.version_info(v)
        c.put('release', info['release'])
        c.put('end_of_life', info['end_of_life'])
        c.put('release_notes', info['notes'])

        # release is also appended to name & description arrays
        c.put('name', [c.release])
        c.put('description', [c.release])

    # update current config status
    def refresh_state(self, step, only=[], skip=[], revise=False):
        self.log.info('Refreshing State')
        has_actions = False
        for ic in self._configs.values():
            # clear away any previous actions
            if hasattr(ic, 'actions'):
                delattr(ic, 'actions')

            dim_keys = set(ic.config_key.split('-'))
            if only and len(set(only) & dim_keys) != len(only):
                self.log.debug("%s SKIPPED, doesn't match --only", ic.config_key)
                continue

            if skip and len(set(skip) & dim_keys) > 0:
                self.log.debug('%s SKIPPED, matches --skip', ic.config_key)
                continue

            ic.refresh_state(step, revise)
            if not has_actions and len(ic.actions):
                has_actions = True

        # re-save with updated actions
        self._save_yaml()
        return has_actions
