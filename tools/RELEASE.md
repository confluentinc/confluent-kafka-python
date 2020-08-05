# Release process

The release process starts with one or more release candidates,
when no more blocking issues needs to be fixed a final tag is created
and the final release is rolled.

confluent-kafka-python uses semver versioning and loosely follows
librdkafka's version, e.g. v0.11.4 for the final release and
v0.11.4rc3 for the 3rd v0.11.4 release candidate.

With the addition of prebuilt binary wheels we make use of travis-ci.org
to build OSX and Linux binaries which are uploaded to Confluent's private
S3 bucket. These artifacts are downloaded by the `tools/download-s3.py` script
and then uploaded manually to PyPi.

**Note**: Python package versions use a lowercase `rcN` suffix to indicate
          release candidates while librdkafka uses `-RCN`. The Python format
          must be used for confluent-kafka-python releases.
          That is to say that while the librdkafka RC is named `v0.11.4-RC3`
          a Python client RC with the same version is named `v0.11.4rc3`.


The following guide uses `v0.11.4rc1` as version for a release candidate,
replace as necessary with your version or remove `rc..` suffix for the
final release.

## 1. Create a RC branch

    git checkout -b v0.11.4rc

## 2. Update librdkafka and OpenSSL versions

### 2.1 Update librdkafka version

Change to the latest librdkafka version in the following files:

 * `.travis.yml`
 * `.appveyor.yml` - this is the librdkafka.redist NuGet version,
                     make sure to strip the leading "v" from the version.
                     E.g., `0.11.4` rather than `v0.11.4`

Change to the latest version of the confluent-librdkafka-plugins in:

 * `tools/install-interceptors.sh` - edit and change version

Commit these changes as necessary:

    $ git commit -m "librdkafka version v0.11.4" .travis.yml .appveyor.yml
    $ git commit -m "confluent-librdkafka-plugins version v0.11.0" tools/install-interceptors.sh


### 2.2 Update OpenSSL version if necessary

As of v0.11.4 OpenSSL is packaged with the python client. It's important
that the OpenSSL version is kept up to date with the latest release.

Update `OPENSSL_VERSION` in `tools/build-openssl.sh` with the latest 1.0.x version
as documented at [openssl.org](https://www.openssl.org/source/)

Commit this change with a commit-message mentioning the version bump:

    $ git commit -m "OpenSSL version bump to 1.0.2o" tools/build-openssl.sh


## 3. Update in-source versions

There are a number of source files that needs to be updated with the
new version number, the easiest way to find these is to search for the
previous version, e.g.: `git grep 0.11`
The version should be set to the final release version, even when doing
RCs, so only needs to be set once for each release.

 * `confluent_kafka/src/confluent_kafka.h` - update the `CFL_VERSION`
    `CFL_VERSION_STR`, change both the string and the hex-representation.
 * `docs/conf.py` - change `release` and `version` variables.
 * `setup.py` - change `version` variable.

Commit these changes with a commit-message containing the version:

    $ git commit -m "Version v0.11.4rc1" confluent_kafka/src/confluent_kafka.c docs/conf.py setup.py


## 4. Create a tag

### 4.1 Create a test tag

Packaging is fragile and is only triggered when a tag is pushed. To avoid
finding out about packaging problems on the RC tag, it is strongly recommended
to first push a test tag to trigger the packaging builds. This tag should
be removed after the build passes.

    $ git tag v0.11.4rc1-test1

Follow the steps in step 5 to push the test tag. When the build is successful,
remove the tag and follow the steps in 4.2 to create a proper tag.

### 4.2 Create proper tag

The tag should be created right after the commit and be named the same as
the version.

    $ git tag v0.11.4rc1


## 5. Push tag and commits

Perform a dry-run push first to make sure the correct branch and only our tag
is pushed.

    $ git push --dry-run --tags origin v0.11.4rc  # tags and branch

Remove `--dry-run` when you're happy with the results.

An alternative is to push branch and tags separately:

    $ git push --dry-run origin v0.11.4rc  # the branch
    $ git push --dry-run --tags origin v0.11.4rc1


## 6. Wait for CI builds

Monitor travis-ci builds by looking atthe *tag* build at
[travis-ci](https://travis-ci.org/confluentinc/confluent-kafka-python)


## 7. Download build artifacts from S3

*Note*: You will need set up your AWS credentials in `~/.aws/credentials` to
        gain access to the S3 bucket.

When the build for all platforms are successful download the resulting
artifacts from S3 using:

    $ cd tools
    $ ./download-s3.py v0.11.4rc1  # replace with your tagged version

The artifacts will be downloaded to `dl-<tag>/`.

## 8. Verify packages

    $ tools/test-wheels.sh tools/dl-v0.11.4rc1


## 10. Open a release PR

## 11. Merge RC branch into master

Once all builds are successful, artifacts have been validated and a team member has approved the PR
proceed with merging the changes into master. Be sure to tidy up the commit log in the PR branch by
squashing related commits (typos, build fixes, etc)

## 12. Upload packages to PyPi

To upload binary packages to test.pypi.org, use:

    $ twine upload -r test dl-v0.11.4rc1/*

To upload binary packages to the proper pypi.org (WARNING!), use:

    $ twine upload dl-v0.11.4rc1/*

For upload source packages make sure to have checked out the correct tag.

Upload source packages to test.pypi.org:

    $ python setup.py sdist upload -r test

Upload source packages to the proper pypi.org (WARNING!):

    $ python setup.py sdist upload


## 13. Verify installation from PyPi

In the same virtualenv as created above:

    $ pip uninstall confluent_kafka

    # For release-candidates specify --pre argument and version-pinning:

    $ pip install --pre confluent_kafka==0.11.4rc1


    # For final releases no --pre or version-pinning, pay
    # attention to the version being picked up, should be the
    # final v0.11.4 release:

    $ pip install confluent_kafka


Verify that the package works and prints the expected version:
    $ python -c 'import confluent_kafka as ck ; print("py:", ck.version(), "c:", ck.libversion())'
    py: ('0.11.4', 721920) c: ('0.11.4-RC1', 722121)



## 14. Create github release

If this was the final release, go to
[github releases](https://github.com/confluentinc/confluent-kafka-python/releases)
and create a new release with the same name as the final release tag (`v0.11.4`).

Add three sections (with markdown `# New features`) to the description:
 * New features
 * Enhancements
 * Fixes

Print the git commit log and copy-paste relevant commits to
the release description, reformatting them as necessary to look nice in
the changelog, put the commits in the appropriate section.

    $ git log --oneline v0.11.3..v0.11.4

Create the release.


That's it, back to the coal mine.
