#!/usr/bin/env python

# Downloads all the ENVO OBOGraph files from the ENVO GitHub repository in an extremely hacky way.
# use -h for help.

ENVO_GITHUB_RELEASES = 'https://api.github.com/repos/EnvironmentOntology/envo/releases'
ENVO_JSON = 'envo.json'

import argparse
import io
import os
import pathlib
import requests
import tarfile

def parseargs():
    parser = argparse.ArgumentParser(
        description='Download the entire set of ENVO OBOGraph files from GitHub.')
    parser.add_argument('--dir', required=True,
        help='the directory in which to store the OBOGraph files.')

    return parser.parse_args()

def main():
    a = parseargs()
    path = pathlib.Path(a.dir)
    path.mkdir(parents=True, exist_ok=True)
    git_rel = requests.get(ENVO_GITHUB_RELEASES).json()
    for r in git_rel:
        tag = r['tag_name']
        print(f'Processing {tag}')
        tarball = requests.get(r['tarball_url'], stream=True).content
        tar = tarfile.open(fileobj=io.BytesIO(tarball))
        for n in tar.getnames():
            if n.endswith(ENVO_JSON) and len(n.split('/')) == 2: # take top level file
                print(f'Saving {n}')
                with open(path / f'envo_{tag}.json', 'wb') as f:
                    # see warnings here
                    # https://docs.python.org/3/library/tarfile.html#tarfile.TarFile.extractall
                    # we assume github are nice guys
                    for b in tar.extractfile(n):
                        f.write(b)

if __name__ == '__main__':
    main()