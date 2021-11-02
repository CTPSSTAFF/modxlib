# modxlib.py - Python class implementing CTPS 'modxlib'
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
_version = "0.2.0"
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


# Functio: open_trip_tables
#
# Summary: Given a directory containing the trip tables in OMX format for the 
#          four daily time periods used by the mode, open them and return 
#          a dictionary with the keys 'am', 'md', 'pm', and 'nt' whose
#          value is the corresponding open OMX file.
#
# Parameters:   tt_dir - directory containing trip table files in OMX format
#
# Return value: A dictionary with the keys 'am', 'md', 'pm', and 'nt' whose
#               value is the corresponding open OMX file.
#
def open_trip_tables(tt_dir):
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

# Function: load_trip_tables
# 
# Summary: Load the trip tables for all time periods the specified list of modes from
#          open OMX files into NumPy arrays.
#          If no list of modes is passed, trip tables for all modes will be returned.
#
# Parameters - tt_omxs - Dictionary, keyed by time period identifier ('am', 'md', 'pm', and 'nt'),
#                        each of whose values is the open OMX trip table file for the corresponding
#                        time period.
#              modes - List of modes (strings) or None
#
# Return value: A two-level dictionary (i.e., first level = time period, second level = mode)
#               the second level of which contain the trip table, in the form of a numPy array,
#               for the [time_period][mode] in question.
#
def load_trip_tables(tt_omxs, modes=None):
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
#              8. in_brmpo - 1 (yes) or 0 (no)
#              9. subregion - abbreviation of Boston Region MPO subregion or NULL
#             10. sector - 'analysis sector' as defined by Bill Kuttner.
#                          Either 'Northeast', 'North', 'Northwest', 'West', 'Southwest',
#                          'South', 'Southeast', 'Central' or ''; the empty string ('')
#                          indicates that the TAZ is outsize of the 164 municipalities
#                          comprising what was once known as the 'CTPS Model Region'.
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
#   6. sector_to_tazes - Given the name of an 'analysis sector', return the list of the records for the TAZes
#      in the sector.
#   7. town_to_tazes(town) - Given the name of a town, return the list of the records for the TAZes in the town.
#      Note: If a town with the same name occurs in more than one state, the  list of TAZes
#      in _all_ such states is returned.
#   8. town_state_to_tazes(town, state) - Given a town and a state abbreviation (e.g., 'MA'),
#      return the list of records for the TAZes in the town
#   9. state_to_tazes(state) - Given a state abbreviation, return the list of records for the TAZes in the state.
#  10. taz_ids(TAZ_record_list) - Given a list of TAZ records, return a list of _only_ the TAZ IDs from those records.
#
# Note:
# For all of the above API calls that return a "list of TAZ records", each returned 'TAZ' is a Python 'dict' containing
# all of the keys (i.e., 'attributes') listed above. To convert such a list to a list of _only_ the TAZ IDs, call taz_ids
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
            retval = pydash.collections.filter_(self._taz_table,
                                                lambda x: x['subregion'].find('SWAP') != -1)
        else:
            retval = pydash.collections.filter_(self._taz_table, lambda x: x['subregion'] == mpo_subregion)
        # end_if
        return retval
    # end_def mpo_subregion_to_tazes()
    
    def sector_to_tazes(self, sector):
        retval = pydash.collections.filter_(self._taz_table, lambda x: x['sector'] == sector)
        return retval
    
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
# NOTE: Everything in Section 3 is specific to TDM19, and should only be used with TDM19 data.
#       These routines will be changed to accord with how transit data is organized in TDM23.
# -- B.K. 30 October 2021
#
_mode_to_metamode_mapping_table = {
    1:  'MBTA Bus - Local Bus',
    2:  'MBTA Bus - Inner Express Bus',
    3:  'MBTA Bus - Outer Express Bus' ,
    4:  'Green Line',
    5:  'Red Line',
    6:  'Mattapan Trolley',
    7:  'Orange Line',
    8:  'Blue Line',
    9:  'Commuter Rail - Fairmount Line',
    10: 'Ferries - Inner Harbor',
    11: 'Ferries - Outer Harbor',
    12: 'Silver Line',
    13: 'Sliver Line',
    14: 'Logan Express',
    15: 'Logan Shuttle',
    16: 'MGH and Other Shuttles',
    17: 'RTA Bus - Brockton RTA',
    18: 'RTA Bus - CATA RTA',
    19: 'RTA Bus - GATRA RTA',
    20: 'RTA Bus - Lowell RTA',
    21: 'RTA Bus - Merrimack RTA',
    22: 'RTA Bus - MetroWest RTA',
    23: 'Private Bus - Bloom',
    24: 'Private Bus - C & J Bus',
    25: 'Private Bus - Cavalier Bus',
    26: 'Private Bus - Concord Coach',
    27: 'Private Bus - Dattco Bus',
    28: 'Private Bus - Plymouth & Brockton',
    29: 'Private Bus - Peter Pan',
    30: 'Private Bus - Yankee',
    31: 'MBTA Subsidized Bus Routes',
    32: 'Commuter Rail - Beverly / Newburyport / Rockport Line',
    33: 'Commuter Rail - Stoughton / Providence Line',
    34: 'Commuter Rail - Greenbush / Plymouth / Kingston / Middleborough Line',
    35: 'Commuter Rail - Haverhill Line',
    36: 'Commuter Rail - Lowell Line',
    37: 'Commuter Rail - Fitchburg Line',
    38: 'Commuter Rail - Framingham / Worcester Line',
    39: 'Commuter Rail - Needham Line',
    40: 'Commuter Rail - Franklin Line',
    41: 'RTA Bus - SRTA RTA',
    42: 'RTA Bus - Worcester RTA',
    43: 'RTA Bus- Pioneer Valley RTA',
    70: 'Walk' }

# Function: mode_to_metamode
#
# Summary: Given one of the 50+ transportation "modes" supported by the TDM, return its "meta mode".
#          For example, the model supports 3 different "modes" for MBTA bus routes; all three of 
#          these have the common "metamode" of 'MBTA_Bus'.
#
# Parameters:  mode  String identifying one of the transporation "modes" supported by the TDM.
#
# Return value: String representing the input mode's "metamode."
#
def mode_to_metamode(mode):
    retval = 'None'
    if mode in _mode_to_metamode_mapping_table:
        return _mode_to_metamode_mapping_table[mode]
    # end_if
    return retval
# mode_to_metamode()


# calculate_total_daily_boardings: Calculate the daily total across all time periods.
#
#    This calculation requires a bit of subtelty, because the number of rows in the four
#    data frames produced by produced in the calling function is NOT necessarily the same. 
#    A brute-force apporach will not work, generally speaking.
#    See comments in the code below for details.
#
# NOTE: This is a helper function for import_transit_assignment (q.v.)
#   
# Parameter: boardings_by_tod - a dict with the keys 'AM', 'MD', 'PM', and 'NT'
#            for which the value of each key is a data frame containing the total
#            boardings for the list of routes specified in the input CSV file.
#
# Return value: The input dict (boardings_by_tod) with an additional key 'daily'
#               the value of which is a dataframe with the total daily boardings
#               for all routes specified in the input CSV across all 4 time periods.
#
def calculate_total_daily_boardings(boardings_by_tod):
    am_results = boardings_by_tod['AM']
    md_results = boardings_by_tod['MD']
    pm_results = boardings_by_tod['PM']
    nt_results = boardings_by_tod['NT']
    
    # Compute the daily sums.
    #
    # Step 1: Join 'am' and 'md' dataframes
    j1 = pd.merge(am_results, md_results, on=['ROUTE', 'STOP'], how='outer', suffixes=('_am', '_md'))
    # Step 1.1 Replace NaN's with 0's
    j1 = j1.fillna(0)

    # Step 1.2 Compute the 'AM' + 'MD' sums
    j1['DirectTransferOff'] = j1['DirectTransferOff_am'] + j1['DirectTransferOff_md']
    j1['DirectTransferOn'] = j1['DirectTransferOn_am'] + j1['DirectTransferOn_md']
    j1['DriveAccessOn'] = j1['DriveAccessOn_am'] + j1['DriveAccessOn_md']
    j1['EgressOff'] = j1['EgressOff_am'] + j1['EgressOff_md']
    j1['Off'] = j1['Off_am'] + j1['Off_md']
    j1['On'] = j1['On_am'] + j1['On_md']
    j1['WalkAccessOn'] = j1['WalkAccessOn_am'] + j1['WalkAccessOn_md'] 
    j1['WalkTransferOff'] = j1['WalkTransferOff_am'] + j1['WalkTransferOff_md']
    j1['WalkTransferOn'] = j1['WalkTransferOn_am'] + j1['WalkTransferOn_md']

    # Step 1.3: Drop un-needed columns
    cols_to_drop = ['DirectTransferOff_am', 'DirectTransferOff_md',
                    'DirectTransferOn_am', 'DirectTransferOn_md',
                    'DriveAccessOn_am', 'DriveAccessOn_md',
                    'EgressOff_am','EgressOff_md',
                    'Off_am', 'Off_md',
                    'On_am', 'On_md',
                    'WalkAccessOn_am', 'WalkAccessOn_md',
                    'WalkTransferOff_am', 'WalkTransferOff_md',
                    'WalkTransferOn_am', 'WalkTransferOn_md'
                    ]
    j1 = j1.drop(columns=cols_to_drop)

    # Step 2: j2 - join 'pm' and 'nt' data frames
    j2 = pd.merge(pm_results, nt_results, on=['ROUTE', 'STOP'], how='outer', suffixes=('_pm', '_nt'))
    # Step 2.1: Replace NaN's with 0's
    j2 = j2.fillna(0)

    # Step 2.2: Compute the 'PM' + 'NT' sums
    j2['DirectTransferOff'] = j2['DirectTransferOff_pm'] + j2['DirectTransferOff_nt']
    j2['DirectTransferOn'] = j2['DirectTransferOn_pm'] + j2['DirectTransferOn_nt']
    j2['DriveAccessOn'] = j2['DriveAccessOn_pm'] + j2['DriveAccessOn_nt']
    j2['EgressOff'] = j2['EgressOff_pm'] + j2['EgressOff_nt']
    j2['Off'] = j2['Off_pm'] + j2['Off_nt']
    j2['On'] = j2['On_pm'] + j2['On_nt']
    j2['WalkAccessOn'] = j2['WalkAccessOn_pm'] + j2['WalkAccessOn_nt'] 
    j2['WalkTransferOff'] = j2['WalkTransferOff_pm'] + j2['WalkTransferOff_nt']
    j2['WalkTransferOn'] = j2['WalkTransferOn_pm'] + j2['WalkTransferOn_nt']

    # Step 2.3: Drop un-needed columns
    cols_to_drop = ['DirectTransferOff_pm', 'DirectTransferOff_nt',
                    'DirectTransferOn_pm', 'DirectTransferOn_nt',
                    'DriveAccessOn_pm', 'DriveAccessOn_nt',
                    'EgressOff_pm','EgressOff_nt',
                    'Off_pm', 'Off_nt',
                    'On_pm', 'On_nt',
                    'WalkAccessOn_pm', 'WalkAccessOn_nt',
                    'WalkTransferOff_pm', 'WalkTransferOff_nt',
                    'WalkTransferOn_pm', 'WalkTransferOn_nt'
                    ]
    j2 = j2.drop(columns=cols_to_drop)

    # Step 3: Join "j1" and "j2" to produce a dataframe with the daily totals
    daily_df = pd.merge(j1, j2, on=['ROUTE', 'STOP'], how='outer', suffixes=('_j1', '_j2'))
    # Step 3.1 : Replace any NaN's with 0's. This line _shouldn't_ be needed - just being extra cautious.
    daily_df = daily_df.fillna(0)

    # Step 3.2 : Compute THE daily sums
    daily_df['DirectTransferOff'] = daily_df['DirectTransferOff_j1'] + daily_df['DirectTransferOff_j2']
    daily_df['DirectTransferOn'] = daily_df['DirectTransferOn_j1'] + daily_df['DirectTransferOn_j2']
    daily_df['DriveAccessOn'] = daily_df['DriveAccessOn_j1'] + daily_df['DriveAccessOn_j2']
    daily_df['EgressOff'] = daily_df['EgressOff_j1'] + daily_df['EgressOff_j2']
    daily_df['Off'] = daily_df['Off_j1'] + daily_df['Off_j2']
    daily_df['On'] = daily_df['On_j1'] + daily_df['On_j2']
    daily_df['WalkAccessOn'] = daily_df['WalkAccessOn_j1'] + daily_df['WalkAccessOn_j2'] 
    daily_df['WalkTransferOff'] = daily_df['WalkTransferOff_j1'] + daily_df['WalkTransferOff_j2']
    daily_df['WalkTransferOn'] = daily_df['WalkTransferOn_j1'] + daily_df['WalkTransferOn_j2']

    # Step 3.3 : Drop un-needed columns
    cols_to_drop = ['DirectTransferOff_j1', 'DirectTransferOff_j2',
                    'DirectTransferOn_j1', 'DirectTransferOn_j2',
                    'DriveAccessOn_j1', 'DriveAccessOn_j2',
                    'EgressOff_j1','EgressOff_j2',
                    'Off_j1', 'Off_j2',
                    'On_j1', 'On_j2',
                    'WalkAccessOn_j1', 'WalkAccessOn_j2',
                    'WalkTransferOff_j1', 'WalkTransferOff_j2',
                    'WalkTransferOn_j1', 'WalkTransferOn_j2'
                    ]
    daily_df = daily_df.drop(columns=cols_to_drop)

    # Finally, we've got the 'daily' total dataframe!
    boardings_by_tod['daily'] = daily_df
    return boardings_by_tod
# end_def calculate_total_daily_boardings()

# import_transit_assignment: Import transit assignment result CSV files for a given scenario.
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
# 3. Return value: a dict of the form:
#    {'AM'    : dataframe with totals for the AM period,
#     'MD'    : datafrme with totals for the MD period,
#     'PM'    : dataframe with totals for the PM period,
#     'NT'    : dataframe with totals for the NT period,
#     'daily' : dataframe with totals for the entire day
#   }
# 
def import_transit_assignment(scenario):
    base = scenario + r'out/'
    tods = ["AM", "MD", "PM", "NT"]
    # At the end of execution of this function, the dictionary variable'TODsums' will contain all the TOD summed results:
    # one key-value-pair for each 'tod' AND the 'daily' total as well.
    
    # The dict 'TODsums' is the return value of this function.
    TODsums = { 'AM' : None, 'MD' : None, 'PM' : None, 'NT' : None }

    # Import CSV files and create sum tables for each T-O-D (a.k.a. 'time period').
    for tod in tods:
        # Get full paths to _all_ CSV files for the current t-o-d.
        x = tod + '/' 
        fq_csv_fns = glob.glob(os.path.join(base,x,r'*.csv'))
        
        # 'tablist' : List of all the dataframes created from reading in the all the CSV files for the current t-o-d
        tablist = []
        for csv_file in fq_csv_fns:
            # Read CSV file into dataframe, set indices, and append to 'tablist'
            tablist.append(pd.read_csv(csv_file).set_index(['ROUTE','STOP']))
        #
  
        # Sum the tables for the current TOD
        TODsums[tod] = reduce(lambda a, b: a.add(b, fill_value=0), tablist)
    # end_for over all tod's

    TODsums =  calculate_total_daily_boardings(TODsums)
    
    # Ensure that the ROUTE and STOP columns of each dataframe in TODsums aren't indices.
    for k in TODsums.keys():
        TODsums[k] = TODsums[k].reset_index()
    #
    return TODsums
# end_def import_transit_assignment()
#
# END of Section 3: Utilities for the transit mode - specific to TDM19.
###############################################################################


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
        dataframe.to_csv(csv_fn, column_list, sep=',')
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

# Function: bbox_of_gdf
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
