# Tools


## download-s3.py

To download CI build artifacts from S3, set up your AWS credentials
and run `tools/download-s3.py <tag|sha1>`, the artifacts will be downloaded
into `dl-<tag|sha1>`.

To upload packages to PyPi (test.pypi.org in this example to be safe), do:

    $ twine upload -r test dl-<tag|sha1>/*

