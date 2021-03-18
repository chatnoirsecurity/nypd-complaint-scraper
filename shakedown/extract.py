from datetime import datetime

ROW_DATA = "C"
UNCHANGED_DATA_BITMASK = "R"
NULL_DATA_BITMASK = "Ø"

METADATA_KEY_NAME = "S"
LOOKUP_TABLE_ID = "DN"

"""
Based on some random googling, this appears to be the 
possible data types.

    enum PrimitiveType {
        Null = 0,
        Text = 1,
        Decimal = 2,
        Double = 3,
        Integer = 4,
        Boolean = 5,
        Date = 6,
        DateTime = 7,
        DateTimeZone = 8,
        Time = 9,
        Duration = 10,
        Binary = 11,
        None = 12,
    }

"""
COL_DATA_TYPE = "T"
TYPE_TEXT = 1
TYPE_DATETIME = 7

# 
# EXTRACTING DATA FROM POWER BI
#
#
# The server will return a large, deeply nested JSON data structure.
#
# 
# Sooooo, if you look at the raw data, you'll quickly see that some 
# rows have different # of columns, or have numbers instead of strings.
# 
# This is due to their data reduction algorithm. As far as I can tell, there 
# are three parts: 
# 
# (1) Lookup table. It takes some values from each column and uses them to create
# a lookup table. The row data is changed to be the array position of the value in the 
# lookup table instead of the string literal. I assume this is to reduce bandwidth/memory on 
# large datasets with lots of duplicate values.
#
# (2) Dropping columns. 
# 
# (a) If a column in a row has the identical value
# as the row before it, the json just drops that column from the result. There's 
# a bitmask to tell you which one.
#
# (b) Not including null values. Exact same idea as (2), just for null values. 
#
# And yes, all three can be used in the same row. :(
# 
# HOW THE BITMASKS WORK
#
# If the JSON has dropped a column from the row array, the row object will have a 
# key with an integer in it which, when converted to binary, will tell you which 
# columns have been dropped and why.
#
# The bitmask is done using big-endian binary, i.e. the last binary digit indicates
# the first column of our data.
#
# For example, say you have 6 columns in your dataset, but the row you're on
# has only four values ([A, C, E, F]). The bitmask integer is 10.
# 
# 10 converted to binary is 001010, which indicates that the second and fourth
# columns are missing from the results. 
# 
# If that's confusing, remember to read the binary number from right to left <--X.
# The *last* binary digit refers to the *first* column in the data array.
# 
# Remember also arrays are counted from zero, so the second ordinal column 
# is at array position 1)

def extract(n):
    # the end result array we'll be returning
    result = []

    # First, we need to pull the bits out of the JSON that we need.
    # 
    # We need:
    #   - the raw compressed data array
    #   - the data lookup table so we can uncompress it
    #   - how many columns are in the query
    #   - column metadata
    #
    # The json paths to get these are all super hairy, so I've hidden them behind
    # functions so you don't have to look at them if you don't want to
    
    query_results = extract_raw_data(n) 
    lookup = extract_data_compression_table(n)

    # We'll need this for loops and calculations.
    num_cols = extract_column_count(n)

    # This presizes an array the same size as our data
    # Part of the data compression algo is dropping columns 
    # if the data is the same as the previous row. So we'll need
    # to keep track of last values at all times.
    last_value_cache = [None] * num_cols

    # Column metadata contains what datatype the column is, and
    # which lookup key we should use to get compressed data
    column_metadata = extract_column_metadata(n)

    for entry in query_results: 
        # this eventually will be the final uncompressed row
        row = []

        raw_data = entry[ROW_DATA]
        
        # we're going to be popping off values from this array
        # in a minute. reversing the array makes this O(2n) instead of O(n^2)
        raw_data.reverse()
        
        # indicates what columns are missing because the data is the same as last row
        # if the mask key is not present, that means no columns removed
        unchanged_mask = entry.get(UNCHANGED_DATA_BITMASK, 0)

        # indicates what columns are missing because they're null
        # if the mask key is not present, that means no columns removed
        null_mask = entry.get(NULL_DATA_BITMASK, 0)

        # for each column in this particular row....
        for n in range(0, num_cols):
            # this will hold the final value of the current cell after we
            # untangle everything
            current_cell = None

            # If you haven't seen the bitshift operator before(<<), this 
            # bit of arcana means that from 0 to NUM_COLS, we can generate a binary
            # number like 000001, 000010, 000100, etc etc.
            #
            # This generates a bitmask for the current row. 
            #
            # if we bitwise AND the current row with the "is data missing" bitmasks,
            # it will return true (non-zero) if col n is missing from our raw data
            #
            # For example, if the unchanged bitmask is 001010, then:
            #
            # col0: 0010010 & 000001 = 000000 = false
            # col1: 0010010 & 000010 = 000010 = true 
            # col2: 0010010 & 000100 = 000000 = false
             
            col_mask = 1 << n

            # is the current column missing from the raw row data? 
            if unchanged_mask & col_mask: # YES, because it's unchanged
                current_cell = last_value_cache[n]
            elif null_mask & col_mask: # YES, because it's null
                current_cell = None
            else: # NO
                current_cell = raw_data.pop()
                # since we have a value, store it in the last value cache
                last_value_cache[n] = current_cell

            metadata = column_metadata[n]
            
            # OK, now that we *have* the data, we need to figure out if we need to 
            # format it, or if it's a reference to a lookup table.

            # This current code is fairly jank, and will crash if new datatypes are
            # added, but I don't think we'll need to worry about that for this project.

            if type(current_cell) == int:

                if metadata[COL_DATA_TYPE] == TYPE_DATETIME:
                    # this is a epoch date (in milliseconds)
                    # so let's format it as ISO date for excel
                    row.append(convert_epoch_time_to_iso_date(current_cell))

                elif metadata[COL_DATA_TYPE] == TYPE_TEXT:
                    # this means our data is in the lookup table
                    # and we should treat the current column as 
                    # an array position, not a literal integer
                    lookup_key = metadata[LOOKUP_TABLE_ID]
                    row.append(lookup[lookup_key][current_cell])

                else:
                    # TODO throw up some kind of alerting
                    # there's prolly other integer-like data 
                    # types out there, including actual integers
                    row.append(current_cell)
            else:
                row.append(current_cell)


        result.append(row)

    return(result)

def extract_raw_data(j):
    # jq   ".results | .[0].result.data.dsr.DS | .[0].PH | .[0].DM0" 
    return j['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]["DM0"]

def extract_data_compression_table(j):
    # jq   ".results | .[0].result.data.dsr.DS | .[0].ValueDicts" 
    return j['results'][0]['result']['data']['dsr']['DS'][0]["ValueDicts"]

def extract_query_metadata(j):
    return j['results'][0]['result']['data']['descriptor']['Select']

def extract_restart_token(j):
    # restart token gives the last row you got, so it "restarts" the query at that point
    # idk why i just work here
    try:
        return j['results'][0]['result']['data']['dsr']['DS'][0]['RT']
    except (IndexError, KeyError):
        # there are no more results?
        return None

def extract_column_count(j):
    m = extract_query_metadata(j)
    return len(m)

def extract_column_metadata(j):
    # why they store this in the first result i do not know
    # this contains (1) what datatype the column is and 
    # (2) what lookup table key should we use to lookup data
    r = extract_raw_data(j)
    return r[0][METADATA_KEY_NAME]

def convert_epoch_time_to_iso_date(epoch):
    # the epoch time we get is in milliseconds, and python doesn't know
    # what to do with that. As far as I can tell, there is no time data
    # in any of the cells, so truncating this to just the date doesn't
    # lose any data in practice
    return datetime.utcfromtimestamp(epoch/1000).date().isoformat()

