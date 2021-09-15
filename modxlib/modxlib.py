# modxlib.py - Python file implementing CTPS 'modxlib'
#
# Author: Ben Krepp (bkrepp@ctps.org)
#

import csv
import numpy as np
import pandas as pd
import geopands as gp
from dbfread import DBF
import pydash

###############################################################################
#
# Section 0: Version identification
#
_version = "0.1.5"
def get_version():
    global _version
    return _version
# end_def

###############################################################################
#
# Section 1: Trip table management
#
_all_time_periods = ['am', 'md', 'pm', 'nt']
_auto_modes = [ 'SOV', 'HOV' ]
_truck_modes = [ 'Heavy_Truck', 'Heavy_Truck_HazMat', 'Medium_Truck', 'Medium_Truck_HazMat', 'Light_Truck' ]
_nm_modes = [ 'Walk', 'Bike' ]
_transit_modes = [ 'DAT_Boat', 'DET_Boat', 'DAT_CR', 'DET_CR', 'DAT_LB', 'DET_LB', 'DAT_RT', 'DET_RT', 'WAT' ]
_all_modes = _auto_modes + _truck_modes + _nm_modes + _transit_modes

# Function: load_tts_as_np_arrays
#
# Summary: Load the trip tables for the specified list of time periods for the specified list of modes as NumPy arrays.
#          If no list of time periods is passed, trip tables for all time periods will be returned.
#          If no list of modes is passed, trip tables for all modes will be returned.
#
# Parameters:   tts             - trip tables, a dict (keys: 'am', 'md', 'pm', and 'nt'),
#                                 each element of which is an '.omx' trip table file that has 
#                                 been opened using the openmatrix library
#               time_periods    - list of time periods (strings), or None
#               mode_list       - list of modes (strings), or None
#
# Return value: A two-level dictionary (i.e., first level = time period, second level = mode)
#               the second level of which contain the trip table, in the form of a numPy array,
#               for the [time_period][mode] in question.
#
def load_tts_as_np_arrays(tts, time_periods=None, mode_list=None):
    if time_periods == None:
        time_periods = _all_time_periods
    #
    if mode_list == None:
        mode_list = _all_modes
    #
    retval = {}
    for period in time_periods:
        retval[period] = None
    #
    for period in time_periods:
        retval[period] = {}
        for mode in mode_list:
            temp = tts[period][mode]
            retval[period][mode] = np.array(temp)
        # end_for
    # end_for
    return retval
# end_def load_tts_for_mode_list_as_np_arrays()

###############################################################################
#
# Section 2: TAZ "shapefile" management
#
# Summary: The class "tazManager" provides a set of methods to perform _attribute_ queries
#          on an ESRI-format "Shapefile" that represents the TAZes in the model region.
#          The attributes are read from the Shapefile's .DBF file; other components of
#          the Shapefile are ignored.
#
#          The Shapefile's .DBF file _must_ contain the following attributes:
#              1. id
#              2. taz
#              3. type - 'I' (internal) or 'E' (external)
#              4. town
#              5. state - state abbreviation, e.g., 'MA'
#              6. town_state - town, state
#              7. mpo - abbreviation of MPO name: 
#              8 in_brmpo - 1 (yes) or 0 (no)
#              9. subregion - abbreviation of Boston Region MPO subregion or NULL
#
#         An object of class tazManager is instantiated by passing in the fully-qualified path
#         to a Shapefile to the class constructor. Hence, it is possible to have more than one
#         instance of this class active simultaneously, should this be needed.
#
# Class tazManager
# Methods:
#   1. __init__(path_to_shapefile) - class constructor
#   2. mpo_to_tazes(mpo) - Given the name (i.e., abbreviation) of an MPO,
#      return a list of the records for the TAZes in it
#   3. brmpo_tazes() - Return the list of the records for the TAZes in the Boston Region MPO
#   4. brmpo_town_to_tazes(town) - Given the name of a town in the Boston Region MPO,
#      return a list of the records for the TAZes in it
#   5. brmpo_subregion_to_tazes(subregion) - Given the name (i.e., abbreviation) of a Boston Region MPO subregion,
#      return a list of the records for the TAZes in it
#   6. town_to_tazes(town) - Given the name of a town, return the list of the records for the TAZes in the town.
#      Note: If a town with the same name occurs in more than one state, the  list of TAZes
#      in _all_ such states is returned.
#   7. town_state_to_tazes(town, state) - Given a town and a state abbreviation (e.g., 'MA'),
#      return the list of records for the TAZes in the town
#   8. state_to_tazes(state) - Given a state abbreviation, return the list of records for the TAZes in the state.
#   9. taz_ids(TAZ_record_list) - Given a list of TAZ records, return a list of _only_ the TAZ IDs from those records.
#
# Note:
# For all of the above API calls that return a "list of TAZ records", each returned 'TAZ' is a Python 'dict' containing
#  all of the keys (i.e., 'attributes') listed above. To convert such a list to a list of _only_ the TAZ IDs, call taz_ids
# on the list of TAZ records.
#
class tazManager():
    _instance = None
    _default_base = r'G:/Data_Resources/modx/canonical_TAZ_shapefile/'
    _default_shapefile_fn = 'candidate_CTPS_TAZ_STATEWIDE_2019.shp'
    _default_fq_shapefile_fn = _default_base + _default_shapefile_fn
    _taz_table = []
    
    def __init__(self, my_shapefile_fn=None):
        # print('Creating the tazManager object.')
        if my_shapefile_fn == None:
            my_shapefile_fn = _default_fq_shapefile_fn
        #
        # Derive name of .dbf file 
        my_dbffile_fn = my_shapefile_fn.replace('.shp', '.dbf')
        dbf_table = DBF(my_dbffile_fn, load=True)
        for record in dbf_table.records:
            new = {}
            new['id'] = int(record['id'])
            new['taz'] = int(record['taz'])
            new['type'] = record['type']
            new['town'] = record['town']
            new['state'] = record['state']
            new['town_state'] = record['town_state']
            new['mpo'] = record['mpo']
            new['in_brmpo'] = int(record['in_brmpo'])
            new['subregion'] = record['subregion']
            self._taz_table.append(new)
        # end_for
        dbf_table.unload()
        print('Number of recrods read = ' + str(len(self._taz_table)))
        return self._instance
    # end_def __init__()
    
    # For debugging during development:
    def _get_tt_item(self, index):
        return self._taz_table[index]
        
    def mpo_to_tazes(self, mpo):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['mpo'] == mpo)
        return retval

    def brmpo_tazes(self):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['in_brmpo'] == 1)
        return retval

    def brmpo_town_to_tazes(self, mpo_town):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['in_brmpo'] == 1 and x['town'] == mpo_town)
        return retval

    def brmpo_subregion_to_tazes(self, mpo_subregion):
        # We have to be careful as some towns are in two subregions,
        # and for these the 'subregion' field of the table contains
        # an entry of the form 'SUBREGION_1/SUBREGION_2'.
        retval = []
        if subregion == 'ICC':
            retval = pydash.collections.filter_(self._taz_table, 
                                                lambda x: x['subregion'].find('ICC') != -1)
        elif subregion == 'TRIC':
            retval = pydash.collections.filter_(self._taz_table, 
                                                lambda x: x['subregion'].find('TRIC') != -1)
        elif subregion == 'SWAP':
            retval = pydash.collections.filter_(self.taz_table,
                                                lambda x: x['subregion'].find('SWAP') != -1)
        else:
            retval = pydash.collections.filter_(self._taz_table, lambda x: x['subregion'] == mpo_subregion)
        # end_if
        return retval
    # def_def mpo_subregion_to_tazes()
    
    # Note: Returns TAZes in town _regardless_ of state.
    def town_to_tazes(self, town):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['town'] == town)
        return retval

    def town_state_to_tazes(self, town, state):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['state'] == state and x['town'] == town)
        return retval

    def state_to_tazes(self, state):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['state'] == state)
        return retval
        
    def taz_ids(self, taz_record_list):
        retval = []
        for taz in taz_record_list:
            retval.append(taz['id'])
        # end_for
        return retval
# end_class tazManager

###############################################################################
#
# Section 3: Miscellaneous utilities for the transit mode
#
_mode_to_metamode_mapping_table = {
    1:  'MBTA_Bus',
    2:  'MBTA_Bus',
    3:  'MBTA_Bus' ,
    4:  'Light_Rail',
    5:  'Heavy_Rail',
    6:  'Heavy_Rail',
    7:  'Heavy_Rail',
    8:  'Heavy_Rail',
    9:  'Commuter_Rail',
    10: 'Ferry',
    11: 'Ferry',
    12: 'Light_Rail',
    13: 'Light_Rail',
    14: 'Shuttle_Express',
    15: 'Shuttle_Express',
    16: 'Shuttle_Express',
    17: 'RTA',
    18: 'RTA',
    19: 'RTA',
    20: 'RTA',
    21: 'RTA',
    22: 'RTA',
    23: 'Private',
    24: 'Private',
    25: 'Private',
    26: 'Private',
    27: 'Private',
    28: 'Private',
    29: 'Private',
    30: 'Private',
    31: 'Private',
    32: 'Commuter_Rail',
    33: 'Commuter_Rail',
    34: 'Commuter_Rail',
    35: 'Commuter_Rail',
    36: 'Commuter_Rail',
    37: 'Commuter_Rail',
    38: 'Commuter_Rail',
    39: 'Commuter_Rail',
    40: 'Commuter_Rail',
    41: 'Commuter_Rail',
    42: 'Commuter_Rail',
    43: 'Commuter_Rail',
    44: 'Commuter_Rail',
    70: 'Walk' }

# Function: mode_to_metamode
#
# Summary: Given one of the 50+ transportation "modes" supported by the TDM, return its "meta mode".
#          For example, the model supports 3 different "modes" for MBTA bus routes; all three of 
#          these have the common "metamode" of 'MBTA_Bus'.
#
# Parameters:   mode  String identifying one of the transporation "modes" supported by the TDM.
#
# Return value: String representing the input mode's "metamode."
#
def mode_to_metamode(mode):
	retval = 'None'
	if mode in _mode_to_metamode_mapping_table:
		return _mode_to_metamode_mapping_table(mode)
	# end_if
	return retval
# mode_to_metamode()

###############################################################################
#
# Section 4: Dataframe and Geo-dataframe utilities
#

# Function: export_df_to_csv
#
# Summary: Export columns in a dataframe to a CSV file.
#          If a list of columns to export isn't specified, export all columns.
#
# Parameters:   dataframe  -  Pandas dataframe
#               csv_fn      - Name of CSV file
#               column_list - List of columns to export, or None
#
# Return value: N/A
#
def export_df_to_csv(dataframe, csv_fn, column_list=None):
	if column_list != None:
		dataframe.to_csv(csv_fn, sep=',', column_list)
	else:
		dataframe.to_csv(csv_fn, sep=',')
# end_def

# Function: export_gdf_to_geojson
#
# Summary: Export a GeoPandas gdataframe to a GeoJSON file.
#
# Parameters:   geo_dataframe  - GeoPandas dataframe
#               geojson_fn     - Name of GeoJSON file
#
# Return value: N/A
#
def export_gdf_to_geojson(geo_dataframe, geojson_fn):
        geo_dataframe.to_file(geojson_fn, driver='GeoJSON')
# end_def

# Function: export_gdf_to_shapefile
#
# Summary: Export a GeoPandas gdataframe to an ESRI-format shapefile
#
# Parameters:   geo_dataframe  - GeoPandas dataframe
#               geojson_fn     - Name of shapefile
#
# Note: Attribute (property) names longer than 10 characters will be truncated,
#       due to the limitations of the DBF file used for Shapefile attributes.
#
# Return value: N/A
#
def export_gdf_to_shapefile(geo_dataframe, shapefile_fn):
        geo_dataframe.to_file(shapefile_fn, driver='ESRI Shapefile')
# end_def

# Function: export_gdf_to_shapefile
#
# Summary: Return the bounding box of all the features in a geo-dataframe.
#
# Parameters:   gdf - a GeoPandas dataframe
#
# Return value: Bounding box of all the features in the input geodataframe.
#               The bounding box is returned as a dictionary with the keys: 
#               { 'minx', 'miny', 'maxx', 'maxy'}.
# 
def bbox_of_gdf(gdf):
    bounds_tuples = gdf['geometry'].map(lambda x: x.bounds)
    bounds_dicts = []
    for t in bounds_tuples:
        temp = { 'minx' : t[0], 'miny' : t[1], 'maxx' : t[2], 'maxy' : t[3] }
        bounds_dicts.append(temp)
    # end_for
    bounds_df = pd.DataFrame(bounds_dicts)
    minx = bounds_df['minx'].min()
    miny = bounds_df['miny'].min()
    maxx = bounds_df['maxx'].max()
    maxy = bounds_df['maxy'].max()
    retval = { 'minx' : minx, 'miny' : miny, 'maxx' : maxx, 'maxy' : maxy }
    return retval
# end_def bbox_of_gdf()

# Function: center_of_bbox
#
# Summary:  Given a geomtric "bounding box", return its center point. 
#
# Parameters: bbox - Bounding box in the form of a dictionary with the keys { 'minx', 'miny', 'maxx', 'maxy'}
#
# Return value: Center point of the bounding box as a dictionary with the keys { 'x' , 'y' }.
#
def center_of_bbox(bbox):
    center_x = bbox['minx'] + (bbox['maxx'] - bbox['minx']) / 2
    center_y = bbox['miny'] + (bbox['maxy'] - bbox['miny']) / 2
    retval = { 'x' : center_x, 'y' : center_y }
    return retval
# end_def center_of_bbox()
