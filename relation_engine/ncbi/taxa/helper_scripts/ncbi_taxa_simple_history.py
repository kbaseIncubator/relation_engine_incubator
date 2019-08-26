#!/usr/bin/python3

# Author: Sean McCorkle

# Calculates the history of each taxa in a number of NCBI taxa datasets.
# TODO DOCS more docs

import os
import re

versions = []

with open( "taxdmp.list" ) as lf:
    for line in lf:
        versions.append( line.strip() )

# read file, get first number from each line,
# return as dictionary of numbers (value True)

def load_d( file ):
    d = {}
    #print( "loading as dir {0}".format( file ) )
    with open( file ) as f:
        for line in f:
            id = re.split( "[^\d]+", line.strip())[0]
            d[id] = True
    return( d )

def load_files( vers ):
    nodes_d = load_d( os.path.join( vers, "nodes.dmp" ) )
    merged_d = load_d( os.path.join( vers, "merged.dmp" ) )
    deleted_d = load_d( os.path.join( vers, "delnodes.dmp" ) )
    return [ nodes_d, merged_d, deleted_d ]

# initialize
nodes, merged, deleted = load_files( versions[0] )

current_state = {}
history = {}
full_history = {}
tot = 0

for n in nodes:
    current_state[n] = "N"
    history[n] = "N"
    full_history[n] = [ [ "N" ] ]
    tot += 1

#print( "{0} nodes".format( tot ) )    

tot = 0    

for n in merged:
    if n in current_state:
       print( "yikes merged {0} already in".format( n ) )
    current_state[n] = "M"
    history[n] = "M"
    full_history[n] = [ [ "M" ] ]
    tot += 1

#print( "{0} merged".format( tot ) )    

tot = 0    

for n in deleted:
    if n in current_state:
       print( "yikes deleted {0} already in".format( n ) )

    current_state[n] = "D"
    history[n] = "D"
    full_history[n] = [ [ "D" ] ]
    tot += 1

#print( "{0} deleted".format( tot ) )    

# off we go

for j in range( 1, len( versions ) ):
    print( "doing {0} {1}".format( j, versions[j] ) )
    nodes, merged, deleted = load_files( versions[j] )

    for n in nodes:
        if n not in current_state:
            current_state[n] = "N"
            history[n] = "CN"
            full_history[n] = [ [ "CN", versions[j] ] ]

        else:
            if current_state[n] != "N":
                #print( "{0} changes {1} -> N".format( n, current_state[n] ) )
                current_state[n] = "N"
                history[n] = history[n] + "N"
                full_history[n] = full_history[n] + [ ["N", versions[j] ] ]

    for n in merged:
        if n not in current_state:
            current_state[n] = "M"
            history[n] = "CM"
            full_history[n] = [ [ "CM", versions[j] ] ]
        else:
            if current_state[n] != "M":
                #print( "{0} changes {1} -> M".format( n, current_state[n] ) )
                current_state[n] = "M"
                history[n] = history[n] + "M"
                full_history[n] = full_history[n] + [ ["M", versions[j] ] ]

    for n in deleted:
        if n not in current_state:
            current_state[n] = "D"
            history[n] = "CD"
            full_history[n] = [ [ "CD", versions[j] ] ]
        else:
            if current_state[n] != "D":
                # print( "{0} changes {1} -> D".format( n, current_state[n] ) )
                current_state[n] = "D"
                history[n] = history[n] + "D"
                full_history[n] = full_history[n] + [ ["D", versions[j] ] ]

# write history

with open( "history.long", "w") as o:

    for n in history:
        o.write( "{0} {1}\n".format( n, history[n] ) )

with open( "full_history.long", "w") as o:

    for n in full_history:
        o.write( "{0} {1}\n".format( n, full_history[n] ) )

# count frequencies of history stirngs
   
hcounts = {}

for h in history.values():
    if h in hcounts:
        hcounts[h] += 1
    else:
        hcounts[h] = 1

for c in hcounts:
    print( "{0} {1}".format( c, hcounts[c] ) )