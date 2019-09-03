"""
Common code for dealing with OBOGraph JSON files.

So far this has only been run with GO Basic.
"""

# TODO TEST
# TODO DOCS better documentation.
# TODO NOW reload GO and see if anything changes

import json as _json
from urllib.parse import urlparse

_OBO_GRAPHS = 'graphs'
_OBO_NODES = 'nodes'
_OBO_EDGES = 'edges'
_OBO_TYPE = 'type'
_OBO_TYPE_CLASS = 'CLASS'
_OBO_TYPE_PROPERTY = 'PROPERTY'
_OBO_TYPE_INDIVIDUAL = 'INDIVIDUAL'
_OBO_TYPES = frozenset([_OBO_TYPE_CLASS, _OBO_TYPE_PROPERTY, _OBO_TYPE_INDIVIDUAL])
_OBO_ID = 'id'
_OBO_LABEL = 'lbl'
_OBO_META = 'meta'
_OBO_DEFINITION = 'definition'
_OBO_DEPRECATED = 'deprecated'
_OBO_BASIC_PROPS = 'basicPropertyValues'
_OBO_COMMENTS = 'comments'
_OBO_COMMENT = 'comment'
_OBO_SUBSETS = 'subsets'
_OBO_SYNONYMS = 'synonyms'
_OBO_XREFS = 'xrefs'
_OBO_SUBJECT = 'sub'
_OBO_OBJECT = 'obj'
_OBO_PREDICATE = 'pred'
_OBO_VALUE = 'val'

_OBO_NAMESPACES = frozenset(['has_obo_namespace'])
_OBO_ALTERNATIVE_IDS = frozenset(['has_alternative_id'])
_OBO_REPLACED_BY = frozenset(['term replaced by'])
_OBO_CONSIDER = frozenset(['consider'])


_OUT_ID = 'id'
_OUT_NODE_TYPE = 'type'
_OUT_FROM = 'from'
_OUT_TO = 'to'
_OUT_EDGE_TYPE = 'type'
_OUT_NAME = 'name'
_OUT_NAMESPACE = 'namespace'
_OUT_ALTERNATIVE_IDS = 'alt_ids'
_OUT_DEFINITION = 'def'
_OUT_COMMENTS = 'comments'
_OUT_SUBSETS = 'subsets'
_OUT_SYNONYMS = 'synonyms'
_OUT_XREFS = 'xrefs'
_OUT_REPLACED_BY = 'replaced_by'
_OUT_CONSIDER = 'consider'

class OBOGraphLoader:
    """
    OBOGraphLoader loads the graph into memory and allows creation of node, edge, and merge
    providers suitable for feeding into a delta load time travelling algorithm.
    """

    def __init__(self, obo, ontology_id_prefix, graph_id=None):
        """
        Create the loader.
        obojson - the OBO graph as loaded from the JSON file.
        ontology_id_prefix - the ID prefix of the ontology to be loaded, e.g. GO or ENVO. This
          is used to exclude nodes and edges that are not part of the ontology.
        graph_id - the ID of the graph in the OBOgraph to load. If there is only one graph this
          may be ommitted.
        """
        if graph_id:
            self._obo = self._get_graph(obo, graph_id)
        elif len(obo[_OBO_GRAPHS]) > 1:
            raise ValueError('Found more than one graph in the OBO file.')
        else:
            self._obo = obo[_OBO_GRAPHS][0]
        
        unknown_types = {n[_OBO_TYPE] for n in self._obo[_OBO_NODES]
            if _OBO_TYPE in n} - _OBO_TYPES # Some ENVO nodes don't have types. AAARRARGAGGG
        if unknown_types:
            raise ValueError(f'Found unprocessable node types {unknown_types}')
        self._ont_prefix = ontology_id_prefix
        self._property_map = self._get_property_map()
        
    def _get_property_map(self):
        ret = {}
        for n in self._obo[_OBO_NODES]:
            # For ENVO, all the nodes without types are property types, so we include them in the
            # property map. I really hope this holds for other ontologies
            # may need to add a flag
            if not n.get(_OBO_TYPE) or n[_OBO_TYPE] == _OBO_TYPE_PROPERTY:
                if _OBO_LABEL in n:
                    ret[n[_OBO_ID]] = n[_OBO_LABEL]
                elif _OBO_META in n and _OBO_BASIC_PROPS in n[_OBO_META]:
                    comments = []
                    for p in n[_OBO_META][_OBO_BASIC_PROPS]:
                        if self._strip_url(p[_OBO_PREDICATE]) == _OBO_COMMENT:
                            comments.append(p[_OBO_VALUE])
                    if len(comments) != 1:
                        raise ValueError(f'Property node {n[_OBO_ID]} has no {_OBO_LABEL} field' +
                            f'and {len(comments)} comments in {_OBO_META}/{_OBO_BASIC_PROPS}')
                    else:
                        ret[n[_OBO_ID]] = comments[0]
                else:
                    raise ValueError(f'Property node {n[_OBO_ID]} has no {_OBO_LABEL} ' +
                        f'and no {_OBO_META}/{_OBO_BASIC_PROPS} fields') 
        return ret

    def _get_graph(self, obo, graph_id):
        for g in obo[_OBO_GRAPHS]:
            if g[_OBO_ID] == graph_id:
                return g
        raise ValueError(f'There is no graph with id {graph_id}')

    def _strip_url(self, string):
        """
        Some strings (like ontology IDs) can be full URLs.
        This function checks to see if the string looks like a http or https url and if so,
        returns the fragment or the last entry in the path if there is no fragment.
        Otherwise it returns the string as is.
        """
        u = urlparse(string)
        if u.scheme != 'http' and u.scheme != 'https':
            return string
        if u.fragment:
            return u.fragment
        return u.path.split('/')[-1].strip()

    def _get_meta_property(self, meta, metakey, target_predicates):
        # totally inefficient but probably doesn't matter
        if not meta or metakey not in meta:
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
        if not meta or metakey not in meta:
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

    def _clean_obo_id(self, id_):
        """
        1) Calls _strip_url()
        2) Splits on _
        if len == 1, return
        if len == 2 and 2nd part is an integer, reassembles with : as the sep.
        """
        # could also check that there's a : in the ID if split len == 1
        idc = self._strip_url(id_)
        idc = idc.split('_')
        if len(idc) == 1:
            return idc[0]
        if len(idc) > 2:
            raise ValueError(f'Could not parse id {id_}')
        try:
            int(idc[1])
        except Exception as _:
            raise ValueError(f'Could not parse id {id_}')
        return f'{idc[0]}:{idc[1]}'

    def _is_valid_node(self, node, deprecated_ok=False):
        n = node
        if n.get(_OBO_TYPE) != _OBO_TYPE_CLASS and n.get(_OBO_TYPE) != _OBO_TYPE_INDIVIDUAL:
            return False
        id_ = self._clean_obo_id(n[_OBO_ID])
        if not id_.startswith(self._ont_prefix):
            return False
        meta = n.get(_OBO_META)
        if not meta:
            return True
        if not deprecated_ok and meta.get(_OBO_DEPRECATED):
            # may want some sort of special loader for loading pre-expired deprecated nodes
            # needs more thought
            return False
        return True

    def get_node_provider(self):
        """
        Returns a generator over the nodes in the graph in time travelling format.
        """
        for n in self._obo[_OBO_NODES]:
            if not self._is_valid_node(n):
                continue
            id_ = self._clean_obo_id(n[_OBO_ID])
            meta = n.get(_OBO_META)
            defi = meta.get(_OBO_DEFINITION) if meta else None
            if defi:
                defi.pop(_OBO_META, None)

            ret = {_OUT_ID: id_,
                   _OUT_NODE_TYPE: n[_OBO_TYPE],
                   _OUT_NAME: n.get(_OBO_LABEL), # some ENVO classes have no label
                   _OUT_NAMESPACE: self._get_meta_property(
                       meta, _OBO_BASIC_PROPS, _OBO_NAMESPACES),
                   _OUT_ALTERNATIVE_IDS: self._get_meta_properties(
                       meta, _OBO_BASIC_PROPS, _OBO_ALTERNATIVE_IDS),
                   _OUT_DEFINITION: defi,
                   _OUT_COMMENTS: meta.get(_OBO_COMMENTS, []) if meta else [],
                   _OUT_SUBSETS: meta.get(_OBO_SUBSETS, []) if meta else [],
                   # may need to translate pred for sys and xrefs? I don't see anything in GO basic
                   _OUT_SYNONYMS: self._clean_meta(meta.get(_OBO_SYNONYMS, [])) if meta else [],
                   _OUT_XREFS: self._clean_meta(meta.get(_OBO_XREFS, [])) if meta else [], 
                   }
            yield ret

    def get_merge_provider(self):
        """
        Returns a generator over the merge edges in the graph in time travelling format.
        """
        for n in self._obo[_OBO_NODES]:
            if not self._is_valid_node(n, deprecated_ok=True):
                continue
            from_ = self._clean_obo_id(n[_OBO_ID])
            meta = n.get(_OBO_META)
            for preds, outpred in [(_OBO_REPLACED_BY, _OUT_REPLACED_BY),
                                   (_OBO_CONSIDER, _OUT_CONSIDER)]:
                for to in self._get_meta_properties(meta, _OBO_BASIC_PROPS, preds):
                    # In GO some IDs are _, some are :. Wow
                    to = self._clean_obo_id(to)
                    # For GO to is not external. If this is untrue, check prefix and skip.
                    yield self._to_edge(from_, to, outpred)

    def _to_edge(self, from_, to, predicate):
        predicate = predicate.replace(' ', '_') # may need to expand this, see
        # https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html
        return {_OUT_ID: f'{from_}::{to}::{predicate}',
                _OUT_FROM: from_,
                _OUT_TO: to,
                _OUT_EDGE_TYPE: predicate
                }

    def get_edge_provider(self):
        """
        Returns a generator over the edges in the graph in time travelling format.
        """
        # At least in GO, edges don't contact deprecated nodes
        # Might need to build up a list of deprecated IDs for other ontologies, or GO later
        for e in self._obo[_OBO_EDGES]:
            sub = e[_OBO_SUBJECT]
            obj = e[_OBO_OBJECT]
            if sub in self._property_map or obj in self._property_map:
                continue # property edge, ignore
            from_ = self._clean_obo_id(sub)
            to = self._clean_obo_id(obj)
            if not from_.startswith(self._ont_prefix) or not to.startswith(self._ont_prefix):
                continue
            pred = e[_OBO_PREDICATE]
            if pred in self._property_map:
                pred = self._property_map[pred]
            else:
                pred = self._strip_url(pred)
            yield self._to_edge(from_, to, pred)

