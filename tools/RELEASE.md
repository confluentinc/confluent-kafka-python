# Release process

The release process starts with one or more tagged release candidates,
when no more blocking issues needs to be fixed a final tag is created
and the final release is rolled.

confluent-kafka-python uses semver versioning and loosely follows
librdkafka's version, e.g. v0.11.4 for the final release and
v0.11.4rc3 for the 3rd v0.11.4 release candidate.

With the addition of prebuilt binary wheels we make use of Semaphore CI
to build OSX, Linux and Windows binaries which are uploaded to build's
artifact directory. These artifacts are downloaded and then uploaded manually
to PyPi.

**Note**: Python package versions use a lowercase `rcN` suffix to indicate
          release candidates while librdkafka uses `-RCN`. The Python format
          must be used for confluent-kafka-python releases.
          That is to say that while the librdkafka RC is named `v0.11.4-RC3`
          a Python client RC with the same version is named `v0.11.4rc3`.


The following guide uses `v0.11.4rc1` as version for a release candidate,
replace as necessary with your version or remove `rc..` suffix for the
final release.

To give you an idea what this process looks like we have this fabricated
time line:

 * Time to prepare for a release.
 * A librdkafka release candidate is available.
 * Check if any open PRs needs merging, do that.
 * Feature freeze - don't merge any more PRs.
 * Create a release candidate branch.
 * Update versions, dependencies (librdkafka), etc.
 * Create test tag to trigger CI builds, test.
 * Work out any release build issues.
 * librdkafka is released, update librdkafka version.
 * Create release candidate tag, wait for CI builds, deploy to test-PyPi.
 * Create PR, get it reviewed.
 * When it is time for final release, merge the PR.
 * On master, create release tag, wait for CI builds, deploy to PyPi.
 * Update confluent.io docs.
 * Announce release.


## FEATURE FREEZE

During the release process no PRs must be merged, unless they're fixing
something that needs to go into the release.
If that's the case the rc branch needs to be rebased on latest master,
or master merged into it, to pick up the new changes.



## 1. Create a RC branch

Make sure your local git clone is up to date and clean:

    $ git checkout master
    $ git pull --rebase --tags origin master

Create an RC branch

     $ git checkout -b v0.11.4rc


## 2. Update CHANGELOG.md

Make sure the top of [CHANGELOG.md](../CHANGELOG.md) has a section
for the upcoming release. See previous releases in CHANGELOG.md for how
to structure and format the release notes.

For our fix and enhancement PRs we typically write the corresponding changelog
entry as part of that PR, but for PRs that have been submitted from the
community we typically need to add changelog entries afterwards.

To find all changes since the last release, do:

    $ git log v0.11.3..    # the last release

Go through the printed git history and identify and changes that are not
already in the changelog and that are relevant to the end-users of the client.
For example, a bug fix for a bug that users would be seeing should have a
corresponding changelog entry, but a fix to the build system is typically not
interesting and can be skipped.

For commits provided by the community we need to provide proper attribution
by looking up the GitHub username for each such change and then `@`-mention
the user from the changelog entry, e.g.:

` * Producer flush() now raises Authenticator errors (@someghuser, #pr-or-issue-number)`



The changelog must also say which librdkafka version is bundled, a link to the
librdkafka release notes, and should also mention some of the most important
changes to librdkafka in that release.

*Note*: `CHANGELOG.md` is in MarkDown format.


## 3. Update librdkafka version

Prebuilt librdkafka library (with all its dependencies included) is bundled
with the Python client in the Python wheels, one wheel for each arch
and platform we support.

The Python release process is usually coupled with the librdkafka release
process, so while building and trying out the Python release candidates
it is likely that a release candidate version of librdkafka will be used.
When all release testing has been done librdkafka will be released first,
and then the Python release branch will be updated to use the final
librdkafka release tag (e.g., v0.11.4) rather than the release candidate
tag (e.g., v0.11.4-RC5).

Change to the latest librdkafka version in the following files:

 * `.semaphore/semaphore.yml`
 * `examples/docker/Dockerfile.alpine`

Change to the latest version of the confluent-librdkafka-plugins in (this step
is usually not necessary):

 * `tools/install-interceptors.sh`

Commit these changes as necessary:

    $ git commit -m "librdkafka version v0.11.4-RC5" .semaphore/semaphore.yml examples/docker/Dockerfile.alpine
    $ git commit -m "confluent-librdkafka-plugins version v0.11.0" tools/install-interceptors.sh


## 4. Update in-source versions

There are a couple of source files that needs to be updated with the
new version number.
The version should be set to the final release version, even when doing
RCs, so it only needs to be set once for each release.

 * `src/confluent_kafka/src/confluent_kafka.h`
    update both `CFL_VERSION` and `CFL_VERSION_STR`.
 * `docs/conf.py` - change `version` variable.
 * `setup.py` - change `version` argument to `setup()`.

Commit these changes with a commit-message containing the version:

    $ git commit -m "Version v0.11.4rc1" src/confluent_kafka/src/confluent_kafka.c docs/conf.py setup.py


## 5. Tag, CI build, wheel verification, upload

The following sections are performed in multiple iterations:

 - **TEST ITERATION** - a number of test/release-candidate iterations where
   test tags are used to trigger CI builds. As many of these as required
   to get fully functional and verified.
 - **CANDIDATE ITERATION** - when the builds test reliable, a release candidate
   RC tag is pushed. The resulting artifacts should be uploaded and tested with
   test.pypi.org.
 - **RELEASE ITERATION** - a release tag is created on master after the
   rc branch has been merged. The resulting artifacts are uploaded to
   official pypi.org.


Repeat the following chapters' TEST ITERATIONs, then submit a PR, get it
reviewed by team mates, perform one or more CANDIDATE ITERATION, and then do a
final RELEASE ITERATION.


### 5.1. Create a tag

Packaging is fragile and is only triggered when a tag is pushed. To avoid
finding out about packaging problems on the RC tag, it is strongly recommended
to first push a test tag to trigger the packaging builds. This tag should
be removed after the build passes.

**Note**: Python package versions use a lowercase `rcN` suffix to indicate
          release candidates while librdkafka uses `-RCN`. The Python format
          must be used for confluent-kafka-python releases.
          That is to say that while the librdkafka RC is named `v0.11.4-RC3`
          a Python client RC with the same version is named `v0.11.4rc3`.

**TEST ITERATION**:

    # Repeat with new tags until all build issues are solved.
    $ git tag v0.11.4rc1-dev2

    # Delete any previous test tag you've created.
    $ git tag tag -d v0.11.4rc1-dev1


**CANDIDATE ITERATION**:

    $ git tag v0.11.4rc1


**RELEASE ITERATION**:

    $ git tag v0.11.4



### 5.2. Push tag and commits

Perform a dry-run push first to make sure the correct branch and only our tag
is pushed.

    $ git push --dry-run --tags origin v0.11.4rc  # tags and branch

An alternative is to push branch and tags separately:

    $ git push --dry-run origin v0.11.4rc  # the branch
    $ git push --dry-run origin v0.11.4rc1  # the tag


Verify that the output corresponds to what you actually wanted to push;
the correct branch and tag, etc.

Remove `--dry-run` when you're happy with the results.


### 5.3. Wait for CI builds to complete

Monitor Semaphore CI builds by looking at the *tag* build at
[Semaphore CI](https://confluentinc.semaphoreci.com/projects/confluent-kafka-python)

CI jobs are flaky and may fail temporarily. If you see a temporary build error,
e.g., a timeout, restart the specific job.

If there are permanent errors, fix them and then go back to 5.1. to create
and push a new test tag. Don't forget to delete your previous test tag.


### 5.4. Download build artifacts

When all CI builds are successful it is time to download the resulting
artifacts from build's Artifact directory located in another tab in the build:

**Note:** The artifacts should be extracted in the folder `tools\dl-<tag>` for
subsequent steps to work properly.

### 5.5. Verify packages

The wheels will have been verified on CI, but it doesn't hurt to verify them
locally (this should be run on OSX with Docker installed):

    $ tools/test-wheels.sh tools/dl-v0.11.4rc1


### 5.5.1. TEST ITERATION: Tidy up and prepare for RC

When all things are looking good it is time to clean up
the git history to look tidy, remove any test tags, and then go back to
5.1 and perform the CANDIDATE ITERATION.



### 5.5.2. CANDIDATE ITERATION: Create PR

Once all test and RC builds are successful and have been verified and you're
ready to go ahead with the release, it is time create a PR to have the
release changes reviewed.

If librdkafka has had a new RC or the final release it is time to update
the librdkafka versions now. Follow the steps in chapter 3.

Do not squash/rebase the RC branch since that would invalidate the RC tags,
so try to keep the commit history tidy from the start in the RC branch.

Create a PR for the RC branch and add team mates as reviewers and wait for
review approval.


### 5.5.3. CANDIDATE ITERATION: Merge PR

Once the PR has been approved and there are no further changes for the release,
merge the PR to master.

Make sure to **Merge** the PR - don't squash, don't rebase - we need the commit
history to stay intact for the RC tags to stay relevant.

With the PR merged to master, check out and update master:

    $ git checkout master
    $ git pull --rebase --tags origin master

    # Make sure your master is clean and identical to origin/master
    $ git status

Now go back to 5.1 and start the final RELEASE ITERATION.


### 5.6. Upload wheel packages to PyPi

**CANDIDATE ITERATION:** To upload binary packages to test.pypi.org, use:

    $ twine upload -r test dl-v0.11.4rc1/*

**RELEASE ITERATION:** To upload binary packages to the proper pypi.org (WARNING!), use:

    $ twine upload dl-v0.11.4rc1/*


### 5.7. Upload source packages to PyPi

When uploading source packages make sure to have checked out the correct tag
and that you do not have any uncommited modifications and that the `build/`
directory is empty.

**CANDIDATE ITERATION:** Upload source packages to test.pypi.org:

    $ python setup.py sdist upload -r test

**RELEASE ITERATION:** Upload source packages to the proper pypi.org (WARNING!):

    $ python setup.py sdist upload


### 5.8. Verify installation from PyPi

In the same virtualenv as created above:

    # Repeat until all versions are uninstalled
    $ pip uninstall confluent_kafka  # repeat until all are removed


**CANDIDATE ITERATION:**

    # For release-candidates specify --pre argument and version-pinning:
    $ pip install --pre confluent_kafka==0.11.4rc1


**RELEASE ITERATION:**

    # For final releases no --pre or version-pinning, pay
    # attention to the version being picked up, should be the
    # final v0.11.4 release:

    $ pip install confluent_kafka


Verify that the package works and prints the expected version:

    $ python -c 'import confluent_kafka as ck ; print("py:", ck.version(), "c:", ck.libversion())'
    py: ('0.11.4', 721920) c: ('0.11.4-RC1', 722121)




## 6. RELEASE ITERATION: Create github release

When the final release is tagged, built and uploaded, go to
[github releases](https://github.com/confluentinc/confluent-kafka-python/releases)
and create a new release with the same name as the final release tag (`v0.11.4`).

Copy the section for this release from `CHANGELOG.md` to the GitHub release.
Use Preview to check that links work as expected.

Create the release.

### 6.1. Announcement

Write a tweet to announce the new release, something like:

    #Apache #Kafka #Python client confluent-kafka-python v0.11.4 released!
    Adds support for <mainline feature> or something about maintenance release.
    <link-to-release-notes-on-github>


### 6.2. Update docs.confluent.io API docs

Create a PR to update the confluent-kafka-python version tag for the
Python API docs on docs.confluent.io.

    # Update the Python API docs to the latest version: includes
      https://github.com/confluentinc/docs and
      https://github.com/confluentinc/docs-platform.

    # Update docs.confluent.io: cut the docs release branch of
      https://github.com/confluentinc/docs-clients-confluent-kafka-python,
      refers to https://confluentinc.atlassian.net/wiki/spaces/TOOLS/pages/2044330444/Create+a+new+version+of+a+documentation+repo#Create-new-release-branches.


### 6.3. Done!

That's it, back to the coal mine.
