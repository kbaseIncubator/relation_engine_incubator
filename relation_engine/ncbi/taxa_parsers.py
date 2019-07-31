"""
Common code for dealing with NCBI taxonomy files.
"""

import re
import unicodedata
from collections import defaultdict
from relation_engine.load_utils import canonicalize

_CANONICAL_IGNORE_SET = {'et','al','and','or','the','a'}
_SEP = r'\s\|\s?'
_SCI_NAME = 'scientific name'

class NCBINodeProvider:
    """
    NCBINodeProvider is an iterable that returns a new NCBI taxonomy node as a dict with each
    iteration.
    It requires access to the names.dmp and nodes.dmp files from a taxonomy dump.
    """

    def __init__(self, names_filehandle, nodes_filehandle):
        """
        Create the provider.
        names_filehandle - the opened names.dmp file.
        nodes_filehandle - the opened nodes.dmp file.
        """
        self._names = self._load_names(names_filehandle)
        self._node_fh = nodes_filehandle

    def _load_names(self, name_file):
        # Could make this use less memory by parsing one nodes worth of entries at a time, since
        # both the names and nodes files are sorted by taxid. YAGNI for now
        name_table = defaultdict(lambda: defaultdict(list))
        for line in name_file:
            tax_id, name, _, category = re.split(_SEP, line)[0:4]
            name_table[tax_id.strip()][category.strip()].append(name.strip())

        return {k: dict(name_table[k]) for k in name_table.keys()}

    def __iter__(self):
        for line in self._node_fh:
            record = re.split(_SEP, line)
            # should really make the ints constants but meh
            id_, rank, gencode = [record[i].strip() for i in [0,2,6]]

            aliases = []
            # May need to move names into separate nodes for canonical search purposes
            for cat in list(self._names[id_].keys()):
                if cat != _SCI_NAME:
                    for nam in self._names[id_][cat]:
                        aliases.append({'category':  cat, 
                                        'name':      nam, 
                                        'canonical': canonicalize(nam, _CANONICAL_IGNORE_SET)
                                        })

            # vertex
            sci_names = self._names[id_][_SCI_NAME]
            if len(sci_names) != 1:
                raise ValueError('Node {} has {} scientific names'.format(id_, len(sci_names)))
            node = {
                    'id':                         id_,
                    'scientific_name':            sci_names[0],
                    'canonical_scientific_name':  canonicalize(
                        sci_names[0], _CANONICAL_IGNORE_SET),
                    'rank':                       rank,
                    'aliases':                    aliases,
                    'ncbi_taxon_id':              int(id_),
                    'gencode':                    int(gencode),
                    }
            
            yield node

class NCBIEdgeProvider:
    """
    NCBIEdgeProvider is an iterable that returns a new NCBI taxonomy edge as a dict where the
    from key is the child ID and the to key the parent ID with each
    iteration.
    It requires access to the nodes.dmp files from a taxonomy dump.
    """

    def __init__(self, nodes_filehandle):
        """
        Create the provider.
        nodes_filehandle - the opened nodes.dmp file.
        """
        self._node_fh = nodes_filehandle

    def __iter__(self):
        for line in self._node_fh:
            record = re.split(_SEP, line)
            # should really make the ints constants but meh
            id_, parent = [record[i].strip() for i in [0,1]]

            if id_ == parent:
                continue  # no self edges
            
            edge = {
                'id': id_ + '_' + parent,
                'from': id_,
                'to': parent
            }
            yield edge