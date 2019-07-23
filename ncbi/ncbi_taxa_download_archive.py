#!/usr/bin/env python

NCBI_HOST = 'ftp.ncbi.nih.gov'
NCBI_TAX_DIR = '/pub/taxonomy/taxdump_archive/'
TAXDUMP_PREFIX = 'taxdmp_'

import argparse
import os
import pathlib
import zipfile
from ftplib import FTP

def parseargs():
    parser = argparse.ArgumentParser(
        description='Download the entire NCBI Taxonomy archives.')
    parser.add_argument('--dir', required=True,
                        help='the directory in which to store the files')

    return parser.parse_args()

def download_and_unzip(ftp, directory, filename):
    zf = os.path.join(directory, filename)
    with open(zf, 'wb') as f:
        ftp.retrbinary(f'RETR {filename}', lambda block: f.write(block))

    dirname = os.path.splitext(filename)[0]
    pathlib.Path(os.path.join(directory, dirname)).mkdir(parents=True, exist_ok=True)
    # pyzip is apparently really slow
    with zipfile.ZipFile(zf) as zipread:
        # should really check for malicious paths here but we assume NCBI are nice guys
        zipread.extractall(dirname)

def main():
    a = parseargs()
    pathlib.Path(a.dir).mkdir(parents=True, exist_ok=True)

    with FTP(NCBI_HOST) as ftp:
        ftp.login()
        ftp.cwd(NCBI_TAX_DIR)
        for f in ftp.mlsd(facts=['size']):
            if f[0].startswith(TAXDUMP_PREFIX):
                download_and_unzip(ftp, a.dir, f[0])

if __name__ == '__main__':
    main()