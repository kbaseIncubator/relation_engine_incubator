"""
Common code for dealing with OBOGraph JSON files.

So far this has only been run with GO Basic.
"""

# TODO TEST
# TODO DOCS better documentation.
# TODO NOW update readme

import json as _json

_OBO_GRAPHS = 'graphs'
_OBO_NODES = 'nodes'
_OBO_EDGES = 'edges'
_OBO_TYPE = 'type'
_OBO_TYPE_CLASS = 'CLASS'
_OBO_TYPE_PROPERTY = 'PROPERTY'
_OBO_TYPES = frozenset([_OBO_TYPE_CLASS, _OBO_TYPE_PROPERTY])
_OBO_ID = 'id'
_OBO_LABEL = 'lbl'
_OBO_META = 'meta'
_OBO_DEFINITION = 'definition'
_OBO_DEPRECATED = 'deprecated'
_OBO_BASIC_PROPS = 'basicPropertyValues'
_OBO_COMMENTS = 'comments'
_OBO_SUBSETS = 'subsets'
_OBO_SYNONYMS = 'synonyms'
_OBO_XREFS = 'xrefs'
_OBO_PREDICATE = 'pred'
_OBO_VALUE = 'val'

_OBO_NAMESPACES = frozenset(['has_obo_namespace'])
_OBO_ALTERNATIVE_IDS = frozenset(['has_alternative_id'])


_OUT_ID = 'id'
_OUT_NAME = 'name'
_OUT_NAMESPACE = 'namespace'
_OUT_ALTERNATIVE_IDS = 'alt_ids'
_OUT_DEFINITION = 'def'
_OUT_COMMENTS = 'comments'
_OUT_SUBSETS = 'subsets'
_OUT_SYNONYMS = 'synonyms'
_OUT_XREFS = 'xrefs'

class OBOGraphLoader:
    """
    OBOGraphLoader loads the graph into memory and allows creation of node, edge, and merge
    providers suitable for feeding into a delta load time travelling algorithm.
    """

    def __init__(self, obo, ontology_id_prefix):
        """
        Create the loader.
        obojson - the OBO graph as loaded from the JSON file.
        ontology_id_prefix - the ID prefix of the ontology to be loaded, e.g. GO or ENVO. This
          is used to exclude nodes and edges that are not part of the ontology.
        """
        if len(obo[_OBO_GRAPHS]) > 1:
            raise ValueError('Found more than one graph in the OBO file.')
        self._obo = obo[_OBO_GRAPHS][0]
        unknown_types = {n[_OBO_TYPE] for n in self._obo[_OBO_NODES]} - _OBO_TYPES
        if unknown_types:
            raise ValueError(f'Found unprocessable node types {unknown_types}')
        self._property_map = {n[_OBO_ID]: n[_OBO_LABEL] for n in self._obo[_OBO_NODES]
            if n[_OBO_TYPE] == _OBO_TYPE_PROPERTY}
        self._ont_prefix = ontology_id_prefix

    def _strip_url(self, string):
        """
        Some strings (like ontology IDs) can be full URLs.
        This function checks to see if any slashes are in the string and if so takes the last
        portion of the string.
        """
        return string.split('/')[-1].strip()

    def _get_meta_property(self, meta, metakey, target_predicates):
        # totally inefficient but probably doesn't matter
        if metakey not in meta:
            return None
        for d in meta[metakey]:
            pred = d[_OBO_PREDICATE]
            if pred in self._property_map:
                pred = self._property_map[pred]
            if pred in target_predicates:
                return d[_OBO_VALUE]
        # could check if there's any more an throw an error?

    def _get_meta_properties(self, meta, metakey, target_predicates):
        # totally inefficient but probably doesn't matter
        ret = []
        if metakey not in meta:
            return ret
        for d in meta[metakey]:
            pred = d[_OBO_PREDICATE]
            if pred in self._property_map:
                pred = self._property_map[pred]
            if pred in target_predicates:
                ret.append(d[_OBO_VALUE])
        return ret

    # modifies in place!
    def _clean_meta(self, docs):
        for d in docs:
            d.pop(_OBO_META, None)
        return docs

    def get_node_provider(self):
        """
        Returns a generator over the nodes in the graph in time travelling format.
        """
        for n in self._obo[_OBO_NODES]:
            if n[_OBO_TYPE] != _OBO_TYPE_CLASS:
                continue
            id_ = self._strip_url(n[_OBO_ID])
            if not id_.startswith(self._ont_prefix):
                continue
            meta = n.get(_OBO_META)
            if not meta:
                raise ValueError(f'No {_OBO_META} field found for node ID {id_}')
            if meta.get(_OBO_DEPRECATED):
                # may want some sort of special loader for loading pre-expired deprecated nodes
                # needs more thought
                continue
            defi = meta.get(_OBO_DEFINITION)
            if defi:
                defi.pop(_OBO_META, None)
                
            ret = {_OUT_ID: id_,
                   _OUT_NAME: n[_OBO_LABEL],
                   _OUT_NAMESPACE: self._get_meta_property(
                       meta, _OBO_BASIC_PROPS, _OBO_NAMESPACES),
                   _OUT_ALTERNATIVE_IDS: self._get_meta_properties(
                       meta, _OBO_BASIC_PROPS, _OBO_ALTERNATIVE_IDS),
                   _OUT_DEFINITION: defi,
                   _OUT_COMMENTS: meta.get(_OBO_COMMENTS, []),
                   _OUT_SUBSETS: meta.get(_OBO_SUBSETS, []),
                   # may need to translate pred for sys and xrefs? I don't see anything in GO basic
                   _OUT_SYNONYMS: self._clean_meta(meta.get(_OBO_SYNONYMS, [])),
                   _OUT_XREFS: self._clean_meta(meta.get(_OBO_XREFS, [])),
                   }
            yield ret

    def get_merge_provider(self):
        # TODO NOW
        return []
    
    def get_edge_provider(self):
        # TODO NOW
        return []

