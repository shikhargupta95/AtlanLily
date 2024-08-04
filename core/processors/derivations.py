#!/usr/bin/env python
from core.exceptions.exceptions import DerivationFailedException
'''
[{
    "path": "foo.bar[]",
    "derivation": "rename",
    "params": {
        "source_key": "rename_sample",
        "destination_key": "renamed_sample1",
        "drop_original": True
    }
}, {
    "path": "foo.bar[]",
    "derivation": "split_key",
    "params": {
        "source_key": split_sample
        "destination_keys": ["split_sample1", "split_sample2"],
        "split_char": "."
        "drop_original": True
    }
}, {
    "path": "foo.bar[]",
    "derivation": "static_value",
    "params": {
        "destination_keys": "static_sample",
        "value": "STATIC_VALUE"
    }
}]
'''


def rename_key(payload, source_key, target_key, drop_original=False):
    if source_key not in payload:
        payload[target_key] = None
    else:
        payload[target_key] = payload[source_key]
        if drop_original:
            del payload[source_key]
    return payload


def assign_static_value(payload, target_key, value):
    payload[target_key] = value
    return payload


def split_key(payload, source_key, split_char, target_keys, drop_original=False):
    if source_key in payload and payload[source_key]:
        items = source_key.split(split_char)
        if len(items) > len(target_keys):
            raise DerivationFailedException("Split Keys failed due to more source items than target keys.")
        else:
            items = [None]*(len(target_keys) - len(items))
            payload.update(dict(zip(target_keys, items)))
            return payload