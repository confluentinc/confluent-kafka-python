#!/usr/bin/env python
"""
Generate wheel metadata RECORD file row for given file.

Usefull in cases where you want to postprocess the wheel archive e.g. include
additional libraries after the wheel has been build.
"""
import argparse
import base64
import csv
import hashlib
import os
import sys

if sys.version_info[0] < 3:

    def native(s, encoding="utf-8"):
        if isinstance(s, unicode):  # noqa: F821
            return s.encode(encoding)
        return s


else:

    def native(s, encoding="utf-8"):
        if isinstance(s, bytes):
            return s.decode(encoding)
        return s


def urlsafe_b64encode(data):
    """urlsafe_b64encode without padding"""
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def gen_record_row(fname, prefix=""):
    writer = csv.writer(sys.stdout, delimiter=",", quotechar='"', lineterminator="\n")
    with open(fname, "rb") as f:
        writer.writerow(
            (
                prefix + fname,
                "sha256="
                + native(urlsafe_b64encode(hashlib.sha256(f.read()).digest())),
                os.stat(fname).st_size,
            )
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fname", help="Path to the library.")
    parser.add_argument("prefix", help="Used to prefix path in the RECORD file.")
    args = parser.parse_args()
    gen_record_row(args.fname, args.prefix)


if __name__ == "__main__":
    main()
