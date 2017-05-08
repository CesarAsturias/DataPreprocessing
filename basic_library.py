import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta

import logging
 
module_logger = logging.getLogger("Data_Library.basic_library")


def resample_data(data, period, time_delta=None, method="mean"):
    """ This function performs a "constant interpolation" in a dataframe over  the index date time range

    :param data: Pandas Dataframe. It must have a datetime index and one or more numeric columns.
    :param period: Desired Granularity:
                   
                   - 1s
                   - 2s
                   - 5s
                   - 10s
                   - 1min
                   - 10min
                   - 30min
                   - 1h
                   - 1d
    :param method: Type of the resampling method:

                    - mean: This method should be used only when the resampling interval is greater than the initial granularity. It will return the mean for each bin
                    - pad: This is intended to be used when the resampling interval is smaller than the initial granularity. It will fill the Nan values with the previous valid value. 
                    - sum: This method should be used only when the resampling interval is greater than the initial granularity. It will return the sum of the values within each bin
    :param time_delta: Offset time
    :type data: Pandas Dataframe object
    :type period: String
    :type time_delta: datetime.timedelta object
    :returns: Pandas Dataframe interpolated constantly
    :rtype: Pandas Dataframe

    """
    logger = logging.getLogger("Data_Library.basic_library.resample_data")
    if method == 'mean':
        logger.info("Mean method")
        if time_delta is None:
            return data.resample(period, convention='end').mean()
        else:
            return data.resample(period, loffset=time_delta, convention='end').mean()
    elif method == 'pad':
        logger.info("pad method")
        return (data.resample(period).pad()).bfill() 
    elif method == 'sum':
        if time_delta is None:
            logger.info("sum_bin method")
            return data.resample(period, convention='end').sum()
        else:
            return data.resample(period, convention='end', loffset=time_delta).sum()
    else:
        logger.info("Not a valid method: %s" % (method))
        print ("Not a valid method: {}".format(method))
        return None

def booleantext2integer(data, column_names):
    """ This function converts a column or list of columns from text to integer values.
    The input text data can be:

        1. True/False
        2. Trip/Normal
        3. Alarm/Normal
        4. On/Off
    
    :param data: Pandas Dataframe, the input data
    :param column_names: The column or columns to be converted to integer values. 
    :type data: Pandas Dataframe
    :type column_names: List
    :returns: Processed Pandas Dataframe
    :rtype: Pandas Dataframe

    """
    logger = logging.getLogger("Data_Library.basic_library.booleantext2integer")
    for name in column_names:
        if not data[data[name].str.contains('True', na = False)].empty:
            data[name] = np.where(data[name] == 'True', 1, 0)
        elif not data[data[name].str.contains('Trip', na = False)].empty:
            data[name] = np.where(data[name] == 'Trip', 1, 0)
        elif not data[data[name].str.contains('Alarm', na = False)].empty:
            data[name] = np.where(data[name] == 'Alarm', 1, 0)
        elif not data[data[name].str.contains('On', na = False)].empty:
            data[name] = np.where(data[name] == 'On', 1, 0)
        else:
            logger.info("Not a valid columns text values")
            print ("Not valid columns values")
            return None
    return data

def integer2booleantext(data, column_names, text_value='On'):
    """ This function converts a column or list of columns from integer values to text.
    The output text data can be:

        1. True/False
        2. Trip/Normal
        3. Alarm/Normal
        4. On/Off
    
    :param data: Pandas Dataframe, the input data
    :param column_names: The column or columns to be converted to integer values. 
    :param text_value: The text type. It can be:
                        
                        1. On --> On/Off
                        2. True --> True/False
                        3. Alarm --> Alarm/Normal
                        4. Trip --> Alarm/Normal

    :type data: Pandas Dataframe
    :type column_names: List
    :type text_value: String
    :returns: Processed Pandas Dataframe
    :rtype: Pandas Dataframe

    """
    logger = logging.getLogger("Data_Library.basic_library.booleantext2integer")
    for name in column_names:
        if text_value == 'True':
            data[name] = np.where(data[name] == 1, 'True', 'False')
        elif text_value == 'On':
            data[name] = np.where(data[name] == 1, 'On', 'Off')
        elif text_value == 'Alarm':
            data[name] = np.where(data[name] == 1, 'Alarm', 'Normal')
        elif text_value == 'Trip':
            data[name] = np.where(data[name] == 1, 'Trip', 'Normal')
        else:
            print ("Not valid text value")
            logger.info("Not a valid text value: %s" % (text_value))
            return None

    return data

def filterbydates(data, init, end):
    """ This function filter a Pandas Dataframe by the init and end dates. The index of the Dataframe must 
    be a datetime index.

    :param data: The data
    :param init: Init date
    :param end: End date
    :type data: Pandas Dataframe whose index is a datetime index
    :type init: datetime.datetime object
    :type end: datetime.datetime object
    :returns: The filtered Dataframe
    :rtype: Pandas Dataframe

    """
    return data.ix[init:end]