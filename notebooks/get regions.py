from __future__ import division, print_function
from collections import Counter

import numpy as np
import pandas as pd
import re
import six
import itertools

from IPython.display import display


# ## Figure out county/PUMA regions

# Electoral data is by county, census data is by PUMA.
# 
# Define regions that are connected components of PUMAs and counties. We'll do it for 2000 and 2010 geographies, as well as merged ones (for multi-year ACS files that use both geographies).

# These are produced by [the MABLE geocorr tool](http://mcdc.missouri.edu/websas/geocorr12.html), by choosing source as `County` and target as either `PUMA (2012)` or `PUMA (2000--used in ACS data thru vintage 2011)`.

data = '../pummeler/data/{}'.format

county_to_puma00 = pd.read_csv(data('county-to-puma00.csv.gz'), encoding = 'latin-1', compression = 'gzip', skiprows=[1], dtype={'county': 'str'})
county_to_puma10 = pd.read_csv(data('county-to-puma10.csv.gz'), encoding = 'latin-1', compression = 'gzip', skiprows=[1], dtype={'county': 'str'})
# county_to_puma00 = pd.read_csv(data('county-to-puma00.csv'), encoding = 'latin-1', skiprows=[1], dtype={'county': 'str'})
# county_to_puma10 = pd.read_csv(data('county-to-puma10.csv'), encoding = 'latin-1', skiprows=[1], dtype={'county': 'str'})


from itertools import count

sub = county_to_puma00[(county_to_puma00.stab == 'LA')
                 & ((county_to_puma00.puma2k == 1801)
                  | (county_to_puma00.puma2k == 1802)
                  | (county_to_puma00.puma2k == 1905))].copy()
sub.puma2k = 77777
county_to_puma00 = pd.concat([county_to_puma00, sub], ignore_index=True)

def get_CCs(pairs):
    A_cc = {}
    B_cc = {}
    i = 0
    counting = list()
    for A, B in pairs:
        A_id = A_cc.get(A)
        B_id = B_cc.get(B)
        
        if A_id is None:
            if B_id is None:
                A_cc[A] = B_cc[B] = i
                i +=1
            else:
                A_cc[A] = B_id
        elif B_id is None:
            B_cc[B] = A_id
        elif A_id != B_id:
            for k, v in A_cc.items():
                if v == B_id:
                    A_cc[k] = A_id
            for k, v in B_cc.items():
                if v == B_id:
                    B_cc[k] = A_id
# If you check counties, states as well as pumas together (we refer the states and pumas as state_puma from now on), you'll notice that ...
#  1 ... some state_puma have the same county, 
#  2 ... some counties have the same state_puma,
#  3 ... and sometimes both cases apply. 
# The above written 'for' loop assigns each unique code pair a key/number (starting with 0), showcasing that they belong together. If case number 1 occurs, the state_puma gets the same number as the corresponding county. In case 2 the county gets the same key from the corresponding state_puma assigned. In case 3 both the state_puma as well as the county get listed again and get a new unique number assigned.
# The following list now bundles up the two sorted lists
    ccs = [(set(), set()) for _ in range(i)]
    for k, v in A_cc.items():
        ccs[v][0].add(k)
    for k, v in B_cc.items():
        ccs[v][1].add(k)
    return [(As, Bs) for As, Bs in ccs if As or Bs]
# In line 41 instead of 'i' stood 'next_cc' which has been defined as 'count().next', but since it always began count on 0 for each iteration (it should count upwards) I simply replaced it with an i iterator.
# Orginally we had xrange instead of range(), but since python 3 they remove xrange() entirely
# iteritems() had to be changed into items()

ccs00_orig = get_CCs(
    (row.county, (row.state, row.puma2k))
    for _, row in county_to_puma00.iterrows())
# iterating all the counties and state_pumas in

ccs10_orig = get_CCs(
    (row.county, (row.state, row.puma12))
    for _, row in county_to_puma10.iterrows())
    
print(len(ccs00_orig))
print(len(ccs10_orig))

# Alaska's electoral districts are different from their counties.
# Too much work to do it, so just pretend Alaska was one CC all along.
def kill_alaska(ccs):
    cs = set()
    s_ps = set()
    to_skip = set()
    for i, (counties, state_pumas) in enumerate(ccs):
        if any(state == 2 for state, puma in state_pumas):
            cs |= counties
            s_ps |= state_pumas
            to_skip.add(i)
# For state number 2, which is Alaska, we set an OR operation: [county,state_puma] list or an empty list
    return [(cs, s_ps)] +         [cc for i, cc in enumerate(ccs) if i not in to_skip]
# With this function Alaska will be counted only once

ccs00 = kill_alaska(ccs00_orig)
ccs10 = kill_alaska(ccs10_orig)
# Now we treat Alaska as one CC

print(len(ccs00))
print(len(ccs10))
# compare this number with the one from line 74, the difference should be 3

st_to_stab = county_to_puma00[['state', 'stab']].drop_duplicates()
# the following list contains all unique state-stab combinations only once thanks to the drop_duplicates() function.
st_to_stab = dict(zip(st_to_stab.state, st_to_stab.stab))
# We first call the state as well as the stab column as two seperate lists and zip them into an iterable, meaning that the state and its corresponding stab are both called together when calling their position.
# Finally, with the dict() function we turn the zipped list into a dictonary. The state list is the index.

from collections import defaultdict
from itertools import count

def cc_names(ccs, fmt='{}_{}'):
    state_counters = defaultdict(lambda: count(1))
    names = []
    for counties, state_pumas in ccs:
        st, = {st for st, puma in state_pumas}
        i = next(state_counters[st])
        names.append(fmt.format(st_to_stab[st], i))
    return names
# Now the year in which the data has been collected will be added to the abbreviations of the state names (named 'stab' in the csv) 
cc_names00 = cc_names(ccs00, '{}_00_{:02}')
cc_names10 = cc_names(ccs10, '{}_10_{:02}')

def region_mappings(ccs, cc_names):
    assert len(ccs) == len(cc_names)
    # Making sure the state-regions and the state-region-names are the same
    county_region = []
    puma_region = []
    for name, (counties, pumas) in zip(cc_names, ccs):
        st, = {st for st, puma in pumas}
        stab = st_to_stab[st]
        
        for c in counties:
            county_region.append((c, name))

        for st, puma in pumas:
            puma_region.append((st, puma, name))
        # Assigning the county-region and the corresponding state-puma the same name
    county_region_df = pd.DataFrame.from_records(
        county_region, columns=['county', 'region'], index=['county']).sort_index()
    puma_region_df = pd.DataFrame.from_records(
        puma_region, columns=['state', 'puma', 'region'], index=['state', 'puma']).sort_index()
    
    return county_region_df, puma_region_df

county_region_00, puma_region_00 = region_mappings(ccs00, cc_names00)
county_region_10, puma_region_10 = region_mappings(ccs10, cc_names10)

county_region_00.to_hdf('regions.h5', 'county_region_00', format='table', complib='blosc', complevel=9, mode='w')
puma_region_00.to_hdf('regions.h5', 'puma_region_00', format='table', complib='blosc', complevel=9)
county_region_10.to_hdf('regions.h5', 'county_region_10', format='table', complib='blosc', complevel=9)
puma_region_10.to_hdf('regions.h5', 'puma_region_10', format='table', complib='blosc', complevel=9)

pd.DataFrame.from_records(list(st_to_stab.items()), columns=['state', 'stab'], index='state') \
  .to_hdf('regions.h5', 'state_to_stab', format='table', complib='blosc', complevel=9)
# since dict_items are not subscriptable, it had to be turned into a list
