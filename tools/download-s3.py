#!/usr/bin/env python3
#
#
# Collects CI artifacts from S3 storage, downloading them
# to a local directory.
#
# The artifacts' folder in the S3 bucket must have the following token
# format:
#  <token>-[<value>]__   (repeat)
#
# Recognized tokens (unrecognized tokens are ignored):
#  p       - project (e.g., "confluent-kafka-python")
#  bld     - builder (e.g., "travis")
#  plat    - platform ("osx", "linux", ..)
#  tag     - git tag
#  sha     - git sha
#  bid     - builder's build-id
#
# Example:
#   p-confluent-kafka-python__bld-travis__plat-linux__tag-__sha-112130ce297656ea1c39e7c94c99286f95133a24__bid-271588764__/confluent_kafka-0.11.0-cp35-cp35m-manylinux1_x86_64.whl


import re
import os
import argparse

import boto3

s3_bucket = 'librdkafka-ci-packages'
dry_run = False


class Artifact (object):
    def __init__(self, arts, path, info=None):
        self.path = path
        # Remove unexpanded AppVeyor $(..) tokens from filename
        self.fname = re.sub(r'\$\([^\)]+\)', '', os.path.basename(path))
        self.lpath = os.path.join(arts.dlpath, self.fname)
        self.info = info
        self.arts = arts
        arts.artifacts.append(self)

    def __repr__(self):
        return self.path

    def download(self, dirpath):
        """ Download artifact from S3 and store in dirpath directory.
            If the artifact is already downloaded nothing is done. """
        if os.path.isfile(self.lpath) and os.path.getsize(self.lpath) > 0:
            return
        print('Downloading %s -> %s' % (self.path, self.lpath))
        if dry_run:
            return
        self.arts.s3_bucket.download_file(self.path, self.lpath)


class Artifacts (object):
    def __init__(self, gitref, dlpath):
        super(Artifacts, self).__init__()
        self.gitref = gitref
        self.artifacts = list()
        # Download directory
        self.dlpath = dlpath
        if not os.path.isdir(self.dlpath):
            if not dry_run:
                os.makedirs(self.dlpath, 0o755)

    def collect_single_s3(self, path, p_match=None):
        """ Collect single S3 artifact
         :param: path string: S3 path
         :param: p_match string: Optional p (project) tag to match
        """

        # The S3 folder (confluent-kafka-python/p-...__bld-../..) contains
        # the tokens needed to perform matching of project, gitref, etc.
        folder = path.split('/')[1]

        rinfo = re.findall(r'(?P<tag>[^-]+)-(?P<val>.*?)__', folder)
        if rinfo is None or len(rinfo) == 0:
            # print('Incorrect folder/file name format for %s' % folder)
            return None

        info = dict(rinfo)

        # Match project
        if p_match is not None and info.get('p', '') != p_match:
            return None

        # Ignore AppVeyor Debug builds
        if info.get('bldtype', '').lower() == 'debug':
            print('Ignoring debug artifact %s' % folder)
            return None

        tag = info.get('tag', None)
        if tag is not None and (len(tag) == 0 or tag.startswith('$(')):
            # AppVeyor doesn't substite $(APPVEYOR_REPO_TAG_NAME)
            # with an empty value when not set, it leaves that token
            # in the string - so translate that to no tag.
            tag = None

        sha = info.get('sha', None)

        # Match tag or sha to gitref
        if (tag is not None and tag == self.gitref) or (sha is not None and sha.startswith(self.gitref)):
            return Artifact(self, path, info)

        return None

    def collect_s3(self, s3_prefix, p_match=None):
        """ Collect and download build-artifacts from S3 based on git reference """
        print('Collecting artifacts matching %s from S3 bucket %s' % (self.gitref, s3_bucket))
        self.s3 = boto3.resource('s3')
        self.s3_bucket = self.s3.Bucket(s3_bucket)
        self.s3_client = boto3.client('s3')

        # note: list_objects will return at most 1000 objects per call,
        #       use continuation token to read full list.
        cont_token = None
        more = True
        while more:
            if cont_token is not None:
                res = self.s3_client.list_objects_v2(Bucket=s3_bucket,
                                                     Prefix=s3_prefix,
                                                     ContinuationToken=cont_token)
            else:
                res = self.s3_client.list_objects_v2(Bucket=s3_bucket,
                                                     Prefix=s3_prefix)

            if res.get('IsTruncated') is True:
                cont_token = res.get('NextContinuationToken')
            else:
                more = False

            for item in res.get('Contents'):
                self.collect_single_s3(item.get('Key'), p_match=p_match)

        for a in self.artifacts:
            a.download(self.dlpath)

    def collect_local(self, path):
        """ Collect artifacts from a local directory possibly previously
        collected from s3 """
        for f in os.listdir(path):
            lpath = os.path.join(path, f)
            if not os.path.isfile(lpath):
                continue
            Artifact(self, lpath)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--no-s3", help="Don't collect from S3", action="store_true")
    parser.add_argument("--dry-run",
                        help="Locate artifacts but don't actually download or do anything",
                        action="store_true")
    parser.add_argument("--directory", help="Download directory (default: dl-<gitref>)", default=None)
    parser.add_argument("tag", help="Tag or git SHA to collect")

    args = parser.parse_args()
    dry_run = args.dry_run
    gitref = args.tag
    if not args.directory:
        args.directory = 'dl-%s' % gitref

    arts = Artifacts(gitref, args.directory)

    if not args.no_s3:
        arts.collect_s3('confluent-kafka-python/', 'confluent-kafka-python')
    else:
        arts.collect_local(arts.dlpath)

    if len(arts.artifacts) == 0:
        raise ValueError('No artifacts found for %s' % arts.gitref)

    print('Collected artifacts:')
    for a in arts.artifacts:
        print(' %s -> %s' % (a.path, a.lpath))
