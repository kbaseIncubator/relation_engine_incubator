""" 
Utilities for loading graph data into the relation engine.
"""

import unicodedata

# assumes there's at least one non-whitespace char in string
def canonicalize(string, ignore_tokens):
    """
    Canonicalizes a string by:
    Lowercasing
    Unicode normalization
    Tokenizing
    Removing non-alphanumeric characters from each end of each token
    Ignoring any tokens in the ignore_tokens set
    """
    # see https://docs.python.org/3/howto/unicode.html#comparing-strings
    normed = unicodedata.normalize('NFD', unicodedata.normalize('NFD', string).casefold())
    # maybe include the full string, but normed, in the returned list?
    tokens = normed.split()
    # TODO TEST for fencepost errors here
    ret = []
    for t in tokens:
        for start in range(len(t)):
            if t[start].isalpha() or t[start].isdigit():
                break
        for end in range(len(t) - 1, -1, -1):
            if t[end].isalpha() or t[end].isdigit():
                break
        if start <= end:
            t = t[start: end + 1]
            if t not in ignore_tokens:
                ret.append(t)
    return ret