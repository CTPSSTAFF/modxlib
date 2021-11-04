"""
modxlib - Python module implementing utilites used by CTPS Model Data Explorer.

The uilities fall into the following categories:
0. Version identification
1. Trip Table management
2. TAZ "shapefile" management
3. Utilities for the transit mode
4. Utilities for working with highway assignment data
5. Utilities for working with "skims"
6. Utilities for working with dataframes and geodataframes

This list will be continued.
"""
# modxlib.py - Python module implementing CTPS 'modxlib'
#
# Author: Ben Krepp (bkrepp@ctps.org)
#

import csv
import numpy as np
import pandas as pd
import geopandas as gp
from dbfread import DBF
import pydash

###############################################################################
#
# Section 0: Version identification
#
_version = "0.2.3"
def get_version():
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

def open_trip_tables(tt_dir):
    """
    Function: open_trip_tables

    Summary: Given a directory containing the trip tables in OMX format for the 
             four daily time periods used by the mode, open them and return 
             a dictionary with the keys 'am', 'md', 'pm', and 'nt' whose
             value is the corresponding open OMX file.

    Args: tt_dir: directory containing trip table files in OMX format

    Returns: A dictionary with the keys 'am', 'md', 'pm', and 'nt' whose
             value is the corresponding open OMX file.
             
    Raises: N/A
    """
    tt_am = tt_dir + 'AfterSC_Final_AM_Tables.omx'
    tt_md = tt_dir + 'AfterSC_Final_MD_Tables.omx'
    tt_pm = tt_dir + 'AfterSC_Final_PM_Tables.omx'
    tt_nt = tt_dir + 'AfterSC_Final_NT_Tables.omx'
    tt_omxs = { 'am' : omx.open_file(tt_am,'r'),
                'md' : omx.open_file(tt_pm,'r'),
                'pm' : omx.open_file(tt_pm,'r'),
                'nt' : omx.open_file(tt_nt,'r') 
              }   
    return tt_omxs
# end_def open_trip_tables()

def load_trip_tables(tt_omxs, modes=None):
    """
    Function: load_trip_tables

    Summary: Load the trip tables for all time periods the specified list of modes from
             open OMX files into NumPy arrays.
             If no list of modes is passed, trip tables for all modes will be returned.

    Args: tt_omxs: Dictionary, keyed by time period identifier ('am', 'md', 'pm', and 'nt'),
                   each of whose values is the open OMX trip table file for the corresponding
                   time period.
           modes: List of modes (strings) or None

    Returns: A two-level dictionary (i.e., first level = time period, second level = mode)
             the second level of which contain the trip table, in the form of a numPy array,
             for the [time_period][mode] in question.

    Raises: N/A
    """ 
    if modes == None:
        modes = _all_modes
    #
    retval  = { 'am' : {}, 'md' : {}, 'pm' : {}, 'nt' : {} }
    for period in _all_time_periods:
        for mode in modes:
            temp = tt_omxs[period][mode]
            retval[period][mode] = np.array(temp)
        # end_for
    # end_for
    return retval
# end_def load_trip_tables()


###############################################################################
#
# Section 2: TAZ "shapefile" management
#
#
class tazManager():
    """
    class: tazManager

    Summary: The class "tazManager" provides a set of methods to perform _attribute_ queries
             on an ESRI-format "Shapefile" that represents the TAZes in the model region.
             The attributes are read from the Shapefile's .DBF file; other components of
             the Shapefile are ignored.
             
    The Shapefile's .DBF file _must_ contain the following attributes:
    1. id
    2. taz
    3. type - 'I' (internal) or 'E' (external)
    4. town
    5. state - state abbreviation, e.g., 'MA'
    6. town_state - town, state
    7. mpo - abbreviation of MPO name: 
    8. in_brmpo - 1 (yes) or 0 (no)
    9. subregion - abbreviation of Boston Region MPO subregion or NULL
    10. sector - 'analysis sector' as defined by Bill Kuttner.
                  Either 'Northeast', 'North', 'Northwest', 'West', 'Southwest',
                  'South', 'Southeast', 'Central' or ''; the empty string ('')
                  indicates that the TAZ is outsize of the 164 municipalities
                  comprising what was once known as the 'CTPS Model Region'.

    An object of class tazManager is instantiated by passing in the fully-qualified path
    to a Shapefile to the class constructor. Hence, it is possible to have more than one
    instance of this class active simultaneously, should this be needed.

    Note: For all of the above methods listed bleo that return a "list of TAZ records", 
    each returned 'TAZ' is a Python 'dict' containing all of the keys (i.e., 'attributes') listed above. 
    To convert such a list to a list of _only_ the TAZ IDs, call taz_ids on the list of TAZ records.
    """
    _instance = None
    _default_base = r'G:/Data_Resources/modx/canonical_TAZ_shapefile/'
    _default_shapefile_fn = 'candidate_CTPS_TAZ_STATEWIDE_2019.shp'
    _default_fq_shapefile_fn = _default_base + _default_shapefile_fn
    _taz_table = []
    
    def __init__(self, my_shapefile_fn=None):
        # print('Creating the tazManager object.')
        if my_shapefile_fn == None:
            my_shapefile_fn = self._default_fq_shapefile_fn
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
            new['sector'] = record['sector']
            self._taz_table.append(new)
        # end_for
        dbf_table.unload()
        print('Number of records read = ' + str(len(self._taz_table)))
        return self._instance
    # end_def __init__()
    
    # For debugging during development:
    def _get_tt_item(self, index):
        return self._taz_table[index]
        
    def mpo_to_tazes(self, mpo):
        """
        mpo_to_tazes(mpo): Given the name (i.e., abbreviation) of an MPO,
                           return a list of the records for the TAZes in it
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['mpo'] == mpo)
        return retval

    def brmpo_tazes(self):
        """
        brmpo_tazes() - Return the list of the records for the TAZes in the Boston Region MPO
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['in_brmpo'] == 1)
        return retval

    def brmpo_town_to_tazes(self, mpo_town):
        """
        brmpo_town_to_tazes(town) - Given the name of a town in the Boston Region MPO,
                                    return a list of the records for the TAZes in it
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['in_brmpo'] == 1 and x['town'] == mpo_town)
        return retval

    def brmpo_subregion_to_tazes(self, mpo_subregion):
        """
        brmpo_subregion_to_tazes(subregion) - Given the name (i.e., abbreviation) of a Boston Region MPO subregion,
                                              return a list of the records for the TAZes in it
        """
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
            retval = pydash.collections.filter_(self._taz_table,
                                                lambda x: x['subregion'].find('SWAP') != -1)
        else:
            retval = pydash.collections.filter_(self._taz_table, lambda x: x['subregion'] == mpo_subregion)
        # end_if
        return retval
    # end_def mpo_subregion_to_tazes()
    
    def sector_to_tazes(self, sector):
        """
        sector_to_tazes - Given the name of an 'analysis sector', return the list of the records for the TAZes
                          in the sector.
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['sector'] == sector)
        return retval
    
    # Note: Returns TAZes in town _regardless_ of state.
    def town_to_tazes(self, town):
        """
         town_to_tazes(town) - Given the name of a town, return the list of the records for the TAZes in the town.
                               Note: If a town with the same name occurs in more than one state, the  list of TAZes
                               in _all_ such states is returned.
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['town'] == town)
        return retval

    def town_state_to_tazes(self, town, state):
        """
        town_state_to_tazes(town, state) - Given a town and a state abbreviation (e.g., 'MA'),
                                           return the list of records for the TAZes in the town.
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['state'] == state and x['town'] == town)
        return retval

    def state_to_tazes(self, state):
        """
        state_to_tazes(state) - Given a state abbreviation, return the list of records for the TAZes in the state.
        """
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['state'] == state)
        return retval
        
    def taz_ids(self, taz_record_list):
        """
        taz_ids(TAZ_record_list) - Given a list of TAZ records, return a list of _only_ the TAZ IDs from those records.
        """
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
# NOTE: Much of the contnets Section 3 is specific to TDM19, and should only be used with TDM19 data.
#       The relevant TDM19-specific portions of Section 3 are clearly marked with the comment
#          *** TDM19-specific code
#       These will be changed to accord with how transit data is organized in TDM23, when this has 
#       become sufficiently well-defined.
#
# -- B.K. 04 November 2021

# *** TDM19-specific code
# he mode-to-metamode mapping machinery is specific to TDM19.
# It may well not be required at all in TDM23.
_mode_to_metamode_mapping_table = {
    1:  'MBTA Bus',
    2:  'MBTA Bus',
    3:  'MBTA Bus' ,
    4:  'Green Line',
    5:  'Red Line',
    6:  'Mattapan Trolley',
    7:  'Orange Line',
    8:  'Blue Line',
    9:  'Commuter Rail',
    10: 'Ferries',
    11: 'Ferries',
    12: 'Silver Line',
    13: 'Sliver Line',
    14: 'Logan Express',
    15: 'Logan Shuttle',
    16: 'MGH and Other Shuttles',
    17: 'RTA Bus',
    18: 'RTA Bus',
    19: 'RTA Bus',
    20: 'RTA Bus',
    21: 'RTA Bus',
    22: 'RTA Bus',
    23: 'Private Bus',
    24: 'Private Bus',
    25: 'Private Bus',
    26: 'Private Bus',
    27: 'Private Bus',
    28: 'Private Bus',
    29: 'Private Bus',
    30: 'Private Bus - Yankee',
    31: 'MBTA Subsidized Bus Routes',
    32: 'Commuter Rail',
    33: 'Commuter Rail',
    34: 'Commuter Rail',
    35: 'Commuter Rail',
    36: 'Commuter Rail',
    37: 'Commuter Rail',
    38: 'Commuter Rail',
    39: 'Commuter Rail',
    40: 'Commuter Rail',
    41: 'RTA Bus',
    42: 'RTA Bus',
    43: 'RTA Bus',
    70: 'Walk' }
    
# *** TDM19-Specific code
def mode_to_metamode(mode):
    """
    Function: mode_to_metamode

    Summary: Given one of the 50+ transportation "modes" supported by the TDM, return its "meta mode".
             For example, the model supports 3 different "modes" for MBTA bus routes; all three of 
             these have the common "metamode" of 'MBTA_Bus'.

    Args: mode: String identifying one of the transporation "modes" supported by the TDM.

    Returns: String representing the input mode's "metamode."

    Raises: N/A
    """
    retval = 'None'
    if mode in _mode_to_metamode_mapping_table:
        return _mode_to_metamode_mapping_table[mode]
    # end_if
    return retval
# mode_to_metamode()

# PLACEHOLDER FOR calculate_total_daily_boardings
#
# Function: calculate_total_daily_boardings
#
# Summary: Calculate the daily total boardings across all time periods.
# This calculation requires a bit of subtelty, because the number of rows in the four
# data frames produced by produced in the calling function is NOT necessarily the same. 
# A brute-force apporach will not work, generally speaking.
# See comments in the code below for details.
#
# NOTE: This is a helper function for import_transit_assignment, which see.
#   
# Args: boardings_by_tod: a dict with the keys 'AM', 'MD', 'PM', and 'NT'
# 	  for which the value of each key is a data frame containing the total
# 	  boardings for the list of routes specified in the input CSV file.
#
# Returns: The input dict (boardings_by_tod) with an additional key 'daily'
# 		 the value of which is a dataframe with the total daily boardings
# 		 for all routes specified in the input CSV across all 4 time periods.
#
# Raises: N/A
#

# PLACEHOLDER FOR import_transit_assignment
#
# Function: import_transit_assignment
# 
# NOTE: This method _should_ work equally well for TDM19 and TDM23.
#       Under TDM19, the directory containing the output CSVs may (and often does)
#       contain multiple CSVs per mode; under TMD23, it will contain only one CSV
#       file per mode. The code will work equally well in both cases.
#       For TDM23, some change to the location of the directory containing the 
#       CSV files may be required.
#
# Summary:  Import transit assignment result CSV files for a given scenario.
#
# 1. Read all CSV files for each time period ('tod'), and caclculate the sums for each time period.
#    Step 1 can be performed as a brute-force sum across all columns, since the number of rows in
#    the CSVs (and thus the dataframes) for any given time period are all the same.
#
# 2. Calculate the daily total across all time periods.
#    Step 2 requires a bit of subtelty, because the number of rows in the data frames produced in 
#    Step 1 is NOT necessarily the same. A brute-force apporach will not work, generally speaking.
#    See comments in the code below for details.
#    NOTE: This step is performed by the helper function calculate_total_daily_boardings.
#
# Args: scenario: path to directory containing transit assignment results in CSV file format
#
# Returns: a dict of the form:
#         { 'AM'    : dataframe with totals for the AM period,
#           'MD'    : datafrme with totals for the MD period,
#           'PM'    : dataframe with totals for the PM period,
#           'NT'    : dataframe with totals for the NT period,
#           'daily' : dataframe with totals for the entire day
#         }
#
# Raises: N/A
#

#
# END of Section 3: Utilities for the transit mode
###############################################################################


###############################################################################
#
# Section 4: Utilities for the highway mode
#
class HighwayAssignmentMgr():
    """ 
    Abstract base class for TDM-version specific class for highway assingment utilities
    """
    tdm_version = ''
    def __init__(self, version_string):
        self.tdm_version = version_string
    #
    def display_version(self):
        print("TDM version = " + self.tdm_version)
    #   
# class HighwayAssignmentMgr

class HighwayAssignmentMgr_TDM19(HighwayAssignmentMgr):
    """ 
    Class for TDM19-specific class for highway assingment utilities
    """
    def __init__(self):
        HighwayAssignmentMgr.__init__(self, "tdm19")
    #
    def load_highway_assignment(self, scenario):
        # stub for now
        pass
    #
# class HighwayAssignmentMgr_TDM19

class HighwayAssignmentMgr_TDM23(HighwayAssignmentMgr):
    """ 
    Class for TDM23-specific class for highway assingment utilities
    """
    def __init__(self):
        HighwayAssignmentMgr.__init__(self, "tdm23")
    #
    def load_highway_assignment(self, scenario):
        # stub for now
        pass
    #
# class HighwayAssignmentMgr_TDM23


###############################################################################
#
# Section 5: Utilities for working with "skims"
#
class SkimsMgr():
    """
    Abstract base class for TDM-version specific class for "skims" utilities
    """
    tdm_version = ''
    def __init__(self, version_string):
        self.tdm_version = version_string
    #
    def display_version(self):
        print("TDM version = " + self.tdm_version)
    #
# class SkimsMgr

class SkimsMgr_TDM19(SkimsMgr):
    def __init__(self):
        SkimsMgr.__init__(self, "tdm19")
    #
    def load_skims(self, scenario):
        # stub for now
        pass
    #
# class SkimsMgr_TDM19

class SkimsMgr_TDM23(SkimsMgr):
    def __init__(self):
        SkimsMgr.__init__(self, "tdm23")
    #
    def load_skims(self, scenario):
        # stub for now
        pass
    #
# class SkimsMgr_TDM23


###############################################################################
#
# Section 6: Dataframe and Geo-dataframe utilities
#
def export_df_to_csv(dataframe, csv_fn, column_list=None):
    """
    Function: export_df_to_csv

    Summary: Export columns in a dataframe to a CSV file.
             If a list of columns to export isn't specified, export all columns.

    Args: dataframe: Pandas dataframe
          csv_fn: Name of CSV file
          column_list: List of columns to export, or None

    Returns: N/A

    Raises: N/A
    """
    if column_list != None:
        dataframe.to_csv(csv_fn, column_list, sep=',')
    else:
        dataframe.to_csv(csv_fn, sep=',')
# end_def

def export_gdf_to_geojson(geo_dataframe, geojson_fn):
        geo_dataframe.to_file(geojson_fn, driver='GeoJSON')
# end_def

def export_gdf_to_shapefile(geo_dataframe, shapefile_fn):
        geo_dataframe.to_file(shapefile_fn, driver='ESRI Shapefile')
# end_def

def bbox_of_gdf(gdf):
    """
    Function: bbox_of_gdf

    Summary: Return the bounding box of all the features in a geo-dataframe.

    Args: gdf: a GeoPandas geo-dataframe

    Returns: Bounding box of all the features in the input geodataframe.
             The bounding box is returned as a dictionary with the keys: 
             { 'minx', 'miny', 'maxx', 'maxy'}.

    Raises: N/A
"""
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

def center_of_bbox(bbox):
    """
    Function: center_of_bbox

    Summary: Given a geometric "bounding box", return its center point. 

    Args: bbox: Bounding box in the form of a dictionary with the keys { 'minx', 'miny', 'maxx', 'maxy' },
                e.g., one returned by bbox_of_gdf.

    Returns: Center point of the bounding box as a dictionary with the keys { 'x' , 'y' }.

    Raises: N/A
    """
    center_x = bbox['minx'] + (bbox['maxx'] - bbox['minx']) / 2
    center_y = bbox['miny'] + (bbox['maxy'] - bbox['miny']) / 2
    retval = { 'x' : center_x, 'y' : center_y }
    return retval
# end_def center_of_bbox()
