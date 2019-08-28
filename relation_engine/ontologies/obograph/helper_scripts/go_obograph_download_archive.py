#!/usr/bin/env python

# Downloads all the GO Basic OBOGraph files from the GO releases site in an extremely hacky way.
# use -h for help.

GO_RELEASES_URL = 'http://release.geneontology.org/'
INDEX_HTML = 'index.html'

import argparse
import os
import pathlib
import requests

def parseargs():
    parser = argparse.ArgumentParser(
        description='Download the entire set of GO Basic OBOGraph files.')
    parser.add_argument('--dir', required=True,
                        help='the directory in which to store the files')

    return parser.parse_args()

def download_obograph(directory, date):
    url = GO_RELEASES_URL + date + '/ontology/go-basic.json'
    print(url)
    gb = requests.get(url).text
    with open(os.path.join(directory, 'go-basic_' + date + '.json'), 'w') as f:
        f.write(gb)

def main():
    a = parseargs()
    pathlib.Path(a.dir).mkdir(parents=True, exist_ok=True)

    root = requests.get(GO_RELEASES_URL).text
    # hacky hacky hacky, could be much less fragile, but who cares
    for l in root.split('\n'):
        if GO_RELEASES_URL in l and INDEX_HTML in l:
            date = l.split(GO_RELEASES_URL)[-1].split('/' + INDEX_HTML)[0]
            download_obograph(a.dir, date)

if __name__ == '__main__':
    main()