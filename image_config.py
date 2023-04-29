# vim: ts=4 et:

import hashlib
import mergedeep
import os
import pyhocon
import shutil

from copy import deepcopy
from datetime import datetime
from pathlib import Path

import clouds
from image_storage import ImageStorage, run
from image_tags import ImageTags


class ImageConfig():

    CONVERT_CMD = {
        'qcow2': ['ln', '-f'],
        'vhd': ['qemu-img', 'convert', '-f', 'qcow2', '-O', 'vpc', '-o', 'force_size=on'],
    }
    # these tags may-or-may-not exist at various times
    OPTIONAL_TAGS = [
        'built', 'uploaded', 'imported', 'import_id', 'import_region', 'published', 'released'
    ]
    STEPS = [
        'local', 'upload', 'import', 'publish', 'release'
    ]

    def __init__(self, config_key, obj={}, log=None, yaml=None):
        self._log = log
        self._yaml = yaml
        self._storage = None
        self.config_key = str(config_key)
        tags = obj.pop('tags', None)
        self.__dict__ |= self._deep_dict(obj)
        # ensure tag values are str() when loading
        if tags:
            self.tags = tags

    @classmethod
    def to_yaml(cls, representer, node):
        d = {}
        for k in node.__dict__:
            # don't serialize attributes starting with _
            if k.startswith('_'):
                continue

            d[k] = node.__getattribute__(k)

        return representer.represent_mapping('!ImageConfig', d)

    @property
    def v_version(self):
        return 'edge' if self.version == 'edge' else 'v' + self.version

    @property
    def local_dir(self):
        return Path('work/images') / self.cloud / self.image_key

    @property
    def local_image(self):
        return self.local_dir / ('image.qcow2')

    @property
    def image_name(self):
        return self.name.format(**self.__dict__)

    @property
    def image_description(self):
        return self.description.format(**self.__dict__)

    @property
    def image_file(self):
        return '.'.join([self.image_name, self.image_format])

    @property
    def image_path(self):
        return self.local_dir / self.image_file

    @property
    def metadata_file(self):
        return '.'.join([self.image_name, 'yaml'])

    def region_url(self, region, image_id):
        return self.cloud_region_url.format(region=region, image_id=image_id, **self.__dict__)

    def launch_url(self, region, image_id):
        return self.cloud_launch_url.format(region=region, image_id=image_id, **self.__dict__)

    @property
    def tags(self):
        # stuff that really ought to be there
        t = {
            'arch': self.arch,
            'bootstrap': self.bootstrap,
            'cloud': self.cloud,
            'description': self.image_description,
            'end_of_life': self.end_of_life,
            'firmware': self.firmware,
            'image_key': self.image_key,
            'name': self.image_name,
            'project': self.project,
            'release': self.release,
            'revision': self.revision,
            'version': self.version
        }
        # stuff that might not be there yet
        for k in self.OPTIONAL_TAGS:
            if self.__dict__.get(k, None):
                t[k] = self.__dict__[k]

        return ImageTags(t)

    # recursively convert a ConfigTree object to a dict object
    def _deep_dict(self, layer):
        obj = deepcopy(layer)
        if isinstance(layer, pyhocon.ConfigTree):
            obj = dict(obj)

        try:
            for key, value in layer.items():
                # some HOCON keys are quoted to preserve dots
                if '"' in key:
                    obj.pop(key)
                    key = key.strip('"')

                # version values were HOCON keys at one point, too
                if key == 'version' and '"' in value:
                    value = value.strip('"')

                obj[key] = self._deep_dict(value)
        except AttributeError:
            pass

        return obj

    def _merge(self, obj={}):
        mergedeep.merge(self.__dict__, self._deep_dict(obj), strategy=mergedeep.Strategy.ADDITIVE)

    def _get(self, attr, default=None):
        return self.__dict__.get(attr, default)

    def _pop(self, attr, default=None):
        return self.__dict__.pop(attr, default)

    # make data ready for Packer ingestion
    def _normalize(self):
        # stringify arrays
        self.name = '-'.join(self.name)
        self.description = ' '.join(self.description)
        self.repo_keys = ' '.join(self.repo_keys)
        self._resolve_motd()
        self._resolve_urls()
        self._stringify_repos()
        self._stringify_packages()
        self._stringify_services()
        self._stringify_dict_keys('kernel_modules', ',')
        self._stringify_dict_keys('kernel_options', ' ')
        self._stringify_dict_keys('initfs_features', ' ')

    def _resolve_motd(self):
        # merge release notes, as apporpriate
        if 'release_notes' not in self.motd or not self.release_notes:
            self.motd.pop('release_notes', None)

        motd = {}
        for k, v in self.motd.items():
            if v is None:
                continue

            # join list values with newlines
            if type(v) is list:
                v = "\n".join(v)

            motd[k] = v

        self.motd = '\n\n'.join(motd.values()).format(**self.__dict__)

    def _resolve_urls(self):
        if 'storage_url' in self.__dict__:
            self.storage_url = self.storage_url.format(v_version=self.v_version, **self.__dict__)

        if 'download_url' in self.__dict__:
            self.download_url = self.download_url.format(v_version=self.v_version, **self.__dict__)

    def _stringify_repos(self):
        # stringify repos map
        #   <repo>: <tag>   # @<tag> <repo> enabled
        #   <repo>: false   # <repo> disabled (commented out)
        #   <repo>: true    # <repo> enabled
        #   <repo>: null    # skip <repo> entirely
        #   ...and interpolate {version}
        self.repos = "\n".join(filter(None, (
            f"@{v} {r}" if isinstance(v, str) else
            f"#{r}" if v is False else
            r if v is True else None
            for r, v in self.repos.items()
        ))).format(version=self.version)

    def _stringify_packages(self):
        # resolve/stringify packages map
        #   <pkg>: true                 # add <pkg>
        #   <pkg>: <tag>                # add <pkg>@<tag>
        #   <pkg>: --no-scripts         # add --no-scripts <pkg>
        #   <pkg>: --no-scripts <tag>   # add --no-scripts <pkg>@<tag>
        #   <pkg>: false                # del <pkg>
        #   <pkg>: null                 # skip explicit add/del <pkg>
        pkgs = {'add': '', 'del': '', 'noscripts': ''}
        for p, v in self.packages.items():
            k = 'add'
            if isinstance(v, str):
                if '--no-scripts' in v:
                    k = 'noscripts'
                    v = v.replace('--no-scripts', '')
                v = v.strip()
                if len(v):
                    p += f"@{v}"
            elif v is False:
                k = 'del'
            elif v is None:
                continue

            pkgs[k] = p if len(pkgs[k]) == 0 else pkgs[k] + ' ' + p

        self.packages = pkgs

    def _stringify_services(self):
        # stringify services map
        #   <level>:
        #       <svc>: true     # enable <svc> at <level>
        #       <svc>: false    # disable <svc> at <level>
        #       <svc>: null     # skip explicit en/disable <svc> at <level>
        self.services = {
            'enable': ' '.join(filter(lambda x: not x.endswith('='), (
                '{}={}'.format(lvl, ','.join(filter(None, (
                    s if v is True else None
                    for s, v in svcs.items()
                ))))
                for lvl, svcs in self.services.items()
            ))),
            'disable': ' '.join(filter(lambda x: not x.endswith('='), (
                '{}={}'.format(lvl, ','.join(filter(None, (
                    s if v is False else None
                    for s, v in svcs.items()
                ))))
                for lvl, svcs in self.services.items()
            )))
        }

    def _stringify_dict_keys(self, d, sep):
        self.__dict__[d] = sep.join(filter(None, (
            m if v is True else None
            for m, v in self.__dict__[d].items()
        )))

    def _is_step_or_earlier(self, s, step):
        log = self._log
        if step == 'state':
            return True

        if step not in self.STEPS:
            return False

        return self.STEPS.index(s) <= self.STEPS.index(step)


    # TODO: this needs to be sorted out for 'upload' and 'release' steps
    def refresh_state(self, step, revise=False):
        log = self._log
        actions = {}
        revision = 0
        step_state = step == 'state'
        step_rollback = step == 'rollback'
        undo = {}

        # enable initial set of possible actions based on specified step
        for s in self.STEPS:
            if self._is_step_or_earlier(s, step):
                actions[s] = True

        # pick up any updated image metadata
        self.load_metadata()

        # TODO: check storage and/or cloud - use this instead of remote_image
        # latest_revision = self.get_latest_revision()

        if (step_rollback or revise) and self.local_image.exists():
            undo['local'] = True



        if step_rollback:
            if self.local_image.exists():
                undo['local'] = True

            if not self.published or self.released:
                if self.uploaded:
                    undo['upload'] = True

                if self.imported:
                    undo['import'] = True

        # TODO: rename to 'remote_tags'?
        # if we load remote tags into state automatically, shouldn't that info already be in self?
        remote_image = clouds.get_latest_imported_tags(self)
        log.debug('\n%s', remote_image)

        if revise:
            if self.local_image.exists():
                # remove previously built local image artifacts
                log.warning('%s existing local image dir %s',
                    'Would remove' if step_state else 'Removing',
                    self.local_dir)
                if not step_state:
                    shutil.rmtree(self.local_dir)

            if remote_image and remote_image.get('published', None):
                log.warning('%s image revision for %s',
                    'Would bump' if step_state else 'Bumping',
                    self.image_key)
                revision = int(remote_image.revision) + 1

            elif remote_image and remote_image.get('imported', None):
                # remove existing imported (but unpublished) image
                log.warning('%s unpublished remote image %s',
                    'Would remove' if step_state else 'Removing',
                    remote_image.import_id)
                if not step_state:
                    clouds.delete_image(self, remote_image.import_id)

            remote_image = None

        elif remote_image:
            if remote_image.get('imported', None):
                # already imported, don't build/upload/import again
                log.debug('%s - already imported', self.image_key)
                actions.pop('local', None)
                actions.pop('upload', None)
                actions.pop('import', None)

            if remote_image.get('published', None):
                # NOTE: re-publishing can update perms or push to new regions
                log.debug('%s - already published', self.image_key)

        if self.local_image.exists():
            # local image's already built, don't rebuild
            log.debug('%s - already locally built', self.image_key)
            actions.pop('local', None)

        else:
            self.built = None

        # merge remote_image data into image state
        if remote_image:
            self.__dict__ |= dict(remote_image)

        else:
            self.__dict__ |= {
                'revision': revision,
                'uploaded': None,
                'imported': None,
                'import_id': None,
                'import_region': None,
                'published': None,
                'artifacts': None,
                'released': None,
            }

        # remove remaining actions not possible based on specified step
        for s in self.STEPS:
            if not self._is_step_or_earlier(s, step):
                actions.pop(s, None)

        self.actions = list(actions)
        log.info('%s/%s = %s', self.cloud, self.image_name, self.actions)

        self.state_updated = datetime.utcnow().isoformat()

    @property
    def storage(self):
        if self._storage is None:
            self._storage = ImageStorage(self.local_dir, self.storage_url, log=self._log)

        return self._storage

    def _save_checksum(self, file):
        self._log.info("Calculating checksum for '%s'", file)
        sha256_hash = hashlib.sha256()
        sha512_hash = hashlib.sha512()
        with open(file, 'rb') as f:
            for block in iter(lambda: f.read(4096), b''):
                sha256_hash.update(block)
                sha512_hash.update(block)

        with open(str(file) + '.sha256', 'w') as f:
            print(sha256_hash.hexdigest(), file=f)

        with open(str(file) + '.sha512', 'w') as f:
            print(sha512_hash.hexdigest(), file=f)

    # convert local QCOW2 to format appropriate for a cloud
    def convert_image(self):
        self._log.info('Converting %s to %s', self.local_image, self.image_path)
        run(
            self.CONVERT_CMD[self.image_format] + [self.local_image, self.image_path],
            log=self._log, errmsg='Unable to convert %s to %s',
            errvals=[self.local_image, self.image_path]
        )
        self._save_checksum(self.image_path)
        self.built = datetime.utcnow().isoformat()

    def upload_image(self):
        self.storage.store(
            self.image_file,
            self.image_file + '.sha256',
            self.image_file + '.sha512'
        )
        self.uploaded = datetime.utcnow().isoformat()

    def save_metadata(self, action):
        os.makedirs(self.local_dir, exist_ok=True)
        self._log.info('Saving image metadata')
        # TODO: save metadata updated timestamp as metadata?
        # TODO: def self.metadata to return what we consider metadata?
        metadata = dict(self.tags)
        self.metadata_updated = datetime.utcnow().isoformat()
        metadata |= {
            'artifacts': self._get('artifacts', None),
            'metadata_updated': self.metadata_updated
        }
        metadata_path = self.local_dir / self.metadata_file
        self._yaml.dump(metadata, metadata_path)
        self._save_checksum(metadata_path)
        if action != 'local' and self.storage:
            self.storage.store(
                self.metadata_file,
                self.metadata_file + '.sha256',
                self.metadata_file + '.sha512'
            )

    def load_metadata(self):
        # TODO: what if we have fresh configs, but the image is already uploaded/imported?
        # we'll need to get revision first somehow
        if 'revision' not in self.__dict__:
            return

        # TODO: revision = '*' for now - or only if unknown?

        # get a list of local matching <name>-r*.yaml?
        metadata_path = self.local_dir / self.metadata_file
        if metadata_path.exists():
            self._log.info('Loading image metadata from %s', metadata_path)
            self.__dict__ |= self._yaml.load(metadata_path).items()

        # get a list of storage  matching <name>-r*.yaml
        #else:
            # retrieve metadata (and image?) from storage_url
            # else:
                # retrieve metadata from imported image

        # if there's no stored metadata, we are in transition,
        #   get a list of imported images matching <name>-r*.yaml
