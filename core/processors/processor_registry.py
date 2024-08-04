#!/usr/bin/env python

import re
import json

import annotations as A
import derivations as D
import transforms as T
import validations as V


TRANSFORMATION_REGISTRY = {
    "change_case": T.change_case,
    "date_standardization": T.date_format
}

DERIVATION_REGISTRY = {
    "rename": D.rename_key,
    "split_key": D.split_key,
    "static_value": D.assign_static_value
}

VALIDATION_REGISTRY = {
    "not_null": V.not_null
}

ANNOTATION_REGISTRY = {
    "standard": A.standard_annotation
}
