#!/usr/bin/env python

from datetime import datetime
from dateutil.parser import parse


def change_case(value, target_case):
    '''
    Transformation for case_change. Can be extended to include other cases as well depending on the use case.
    :param value: str, value to which the transformation is to be applied
    :param target_case: str, the target case to which the value is to be transformed
    :return: str
    '''
    if target_case == "upper":
        if value and isinstance(value, str):
            return value.upper()
    if target_case == "lower":
        if value and isinstance(value, str):
            return value.lower()
def date_format(value, target_format, source_format=None):
    '''
    Transformation for date format standardization
    :param value: str, value to which the transformation is to be applied
    :param target_format: str, The pythonic datetime format to which the date is to be converted to
    :param source_format: str, (optional) The current pythonic datetime format
    :return: str
    '''
    if not source_format:
        datetime_obj = parse(value)
    else:
        datetime_obj = datetime.strptime(value, source_format)
    return datetime_obj.strftime(target_format)