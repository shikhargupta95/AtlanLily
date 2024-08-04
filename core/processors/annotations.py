#!/usr/bin/env python

def standard_annotation(value, annotation, operator, threshold, mode="dynamic"):
    if mode.lower() == "static":
        return annotation

    elif mode.lower() == "dynamic":
        if operator == "equals":
            return annotation if value == threshold else None
        elif operator == "gte":
            return annotation if value >= threshold else None
        elif operator == "gt":
            return annotation if value > threshold else None
        elif operator == "lte":
            return annotation if value <= threshold else None
        elif operator == "lt":
            return annotation if value < threshold else None
        elif operator == "not_equals":
            return annotation if value != threshold else None


