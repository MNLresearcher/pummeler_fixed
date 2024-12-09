from __future__ import print_function, division
from six import iteritems, next
from six.moves import xrange

import sys
sys.path.append('..')

import pandas as pd

from pummeler.data import geocode_data

county_region_00 = geocode_data('county_region_00')
county_region_10 = geocode_data('county_region_10')

county_region_00.head()

counties = set(county_region_00.index)

counties == set(county_region_10.index)

from itertools import count
from functools import partial

def get_CCs(pairs):
    A_cc = {}
    B_cc = {}
    
    next_cc = partial(next, count())
    
    for A, B in pairs:
        A_id = A_cc.get(A)
        B_id = B_cc.get(B)
        
        if A_id is None:
            if B_id is None:
                A_cc[A] = B_cc[B] = next_cc()
            else:
                A_cc[A] = B_id
        elif B_id is None:
            B_cc[B] = A_id
        elif A_id != B_id:
            for k, v in iteritems(A_cc):
                if v == B_id:
                    A_cc[k] = A_id
            for k, v in iteritems(B_cc):
                if v == B_id:
                    B_cc[k] = A_id
    
    ccs = [(set(), set()) for _ in range(next_cc())]
    for k, v in iteritems(A_cc):
        ccs[v][0].add(k)
    for k, v in iteritems(B_cc):
        ccs[v][1].add(k)
    return [(As, Bs) for As, Bs in ccs if As or Bs]

from collections import defaultdict
from itertools import count

def cc_names(ccs, fmt='{}_{}'):
    state_counters = defaultdict(lambda: count(1))
    names = []
    for counties, state_regions in ccs:
        st, = {r[:2] for r in state_regions}
        i = next(state_counters[st])
        names.append(fmt.format(st, i))
    return names

def region_mappings(ccs, cc_names):
    assert len(ccs) == len(cc_names)
    county_region = []
    sub_super = []
    for name, (counties, subregions) in zip(cc_names, ccs):        
        for c in counties:
            county_region.append((c, name))

        for r in subregions:
            sub_super.append((r, name))
    
    county_region_df = pd.DataFrame.from_records(
        county_region, columns=['county', 'merged_region'], index=['county']).sort_index()
    sub_super_df = pd.DataFrame.from_records(
        sub_super, columns=['region', 'merged_region'], index=['region']).sort_index()
    
    return county_region_df, sub_super_df

merged_ccs = get_CCs(
    (c, r) for d in [county_region_00, county_region_10]
           for c, r in iteritems(d.region))

merged_cc_names = cc_names(merged_ccs, '{}_merged_{:02}')

county_superregion, region_superregion = region_mappings(merged_ccs, merged_cc_names)

county_superregion.head()

region_superregion.head()

#fn = '../pummeler/data/regions.h5'
# Since the above written directory isn't working I simply copied the region.h5 file, which was generated from the 'get regions.py' file in the .pummeler/Data folder, and paste it into the folder 'notebooks'
county_superregion.to_hdf('regions.h5', 'county_superregion', format='table', complib='blosc', complevel=9)
region_superregion.to_hdf('regions.h5', 'region_superregion', format='table', complib='blosc', complevel=9)

