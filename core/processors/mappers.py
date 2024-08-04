#!/usr/bin/env python

import abc
import logging

from processor_registry import TRANSFORMATION_REGISTRY, DERIVATION_REGISTRY, ANNOTATION_REGISTRY, VALIDATION_REGISTRY
from core.exceptions.exceptions import FieldMapperException, ValidationFailedException

class AbstractFieldMapper:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def field_mapper(self, payload, conf):
        pass


class GenericFieldMapper(AbstractFieldMapper):
    '''
    The field mapper is responsible for parsing the config and applying the various actions on a document in the pre or
    post-processing stage. The functionality is a part of the core package that is to be installed separately and used
    across all modules

    Order of operations in the configurations:
    * Transformations
    * Derivations
    * Validations
    * Annotations

    Config Structure:

        {
            "transformations": [{
                "path": "<path to the key on which the action is to be applied>",
                "transformation": "<the transformation to be applied>",
                "params": "<the keyword arguments to the transformation method>"
            }],
            "derivations": [{
                "path": "<path to the key on which the action is to be applied>",
                "derivation": "<the derivation to be applied>",
                "params": "<the keyword arguments to the derivation method>"
            }],
            "validations": [{
                "path": "<path to the key on which the action is to be applied>",
                "validation": "<the validation to be applied>",
                "params": "<the keyword arguments to the validation method>"
            }],
            "annotations": [{
                "path": "<path to the key on which the action is to be applied>",
                "annotation": "<the annotation to be applied>",
                "params": "<the keyword arguments to the annotation method>"
            }]
        }

    Sample Config:

        {
            "transformations": [{
                "path": "foo.bar[].upper_sample",
                "transformation": "change_case",
                "params": {
                    "target_case": "lower"
                }
            },{
                "path": "foo.bar[].date_sample",
                "transformation": "date_standardization",
                "params": {
                    "source_format": "%Y/%m/%d %H:%M:%S"
                    "target_format": "%Y-%m-%dT%H:%M:%S.%fZ"
                }
            }],
            "derivations": [{
                "path": "foo.bar[]",
                "derivation": "rename",
                "params": {
                    "source_key": "rename_sample",
                    "target_key": "renamed_sample1",
                    "drop_original": True
                }
            }, {
                "path": "foo.bar[]",
                "derivation": "split_key",
                "params": {
                    "source_key": split_sample
                    "target_keys": ["split_sample1", "split_sample2"],
                    "split_char": "."
                    "drop_original": True
                }
            }, {
                "path": "foo.bar[]",
                "derivation": "static_value",
                "params": {
                    "target_key": "static_sample",
                    "value": "STATIC_VALUE"
                }
            }],
            "validations": [{
                "path": "foo.bar[].validation_sample",
                "validation": "not_null",
                "params": {
                    "on_failure": "raise_error"
                }
            }],
            "annotations": [{
                "path": "annotation_key",
                "annotation": "standard",
                "params": {
                    "mode": "dynamic",
                    "annotation": "PII",
                    "operator": "equals",
                    "threshold": "name"
                }
            }]
        }
    '''
    def __init__(self):
        self.__transformations = TRANSFORMATION_REGISTRY
        self.__derivations = DERIVATION_REGISTRY
        self.__validations = VALIDATION_REGISTRY
        self.__annotations = ANNOTATION_REGISTRY

        self.__current_action = None
        self.__current_action_params = None

    def __parse_payload(self, dct, path_tokens=None, extract_only=False):
        '''
        Internal method to recursively parse nested jsons and apply processing to them.
        :param dct: dict/list, The source dict to which an action is to be applied
        :param path_tokens: list, the tokenised path to the key to which the processing is to be applied
        :param extract_only: bool, flag to indicate that we just need to extract the value at a given path
        :return: dict
        '''
        if not isinstance(dct, bool) and not dct:
            return dct
        if not path_tokens:
            return self.__current_action(dct, **self.__current_action_params)
        if len(path_tokens) == 1:
            if path_tokens[0].endswith('[]'):
                token = path_tokens[0][:-2]
                if extract_only:
                    return dct.get(token, [])
                temp = []
                for item in dct.get(token, []):
                    processed_item = self.__current_action(item, **self.__current_action_params)
                    temp.append(processed_item)
                dct[token] = temp
            else:
                token = path_tokens[0]
                if extract_only:
                    return dct.get(token, [])
                if token in dct:
                    processed_item = self.__current_action(dct[token], **self.__current_action_params)
                    dct[path_tokens[0]] = processed_item
        else:
            if path_tokens[0].endswith('[]'):
                token = path_tokens[0][:-2]
                temp = []
                for item in dct.get(token, []):
                    temp.append(self.__parse_payload(item, path_tokens[1:], extract_only))
                if extract_only:
                    return temp
                dct[token] = temp
            else:
                if not path_tokens:
                    return self.__current_action(dct, **self.__current_action_params)
                if path_tokens[0] in dct:
                    value = self.__parse_payload(dct[path_tokens[0]], path_tokens[1:], extract_only)
                    if extract_only:
                        return value
                    dct[path_tokens[0]] = value
        return dct

    def field_mapper(self, payload, conf):
        '''
        A transform to map keys in a specified manner. If a config key is set for an item, the transform will be applied
        otherwise, it'll act as a passthrough. More specific field mappers can be added according to use case.
        :param payload: dict, the payload on which the actions are to be performed
        :param conf: dict, A per action config containing the keys and the params for each action
        :return: dict
        '''
        error_dct = {
            "validations": []
        }
        transforms = conf.get("transformations", [])
        for transform in transforms:
            try:
                current_transform = transform["transformation"]
                self.__current_action = self.__transformations[current_transform]
            except KeyError as e:
                logging.error(f"Transformation not found: {current_transform}. Please add the transform to the registry and restart the service")
                raise FieldMapperException(f"Transformation not found: {current_transform}.")
            self.__current_action_params = transform.get("params", {})
            path_tokens = transform["path"].split(".")
            payload = self.__parse_payload(payload, path_tokens)

        derivations = conf.get("derivations", [])
        for derivation in derivations:
            try:
                current_derivation = derivation["derivation"]
                self.__current_action = self.__derivations[current_derivation]
            except KeyError as e:
                logging.error(
                    f"Derivation not found: {current_derivation}. Please add the derivation to the registry and restart the service")
                raise FieldMapperException(f"Derivation not found: {current_derivation}.")
            self.__current_action_params = derivation.get("params", {})
            path_tokens = derivation["path"].split(".")
            payload = self.__parse_payload(payload, path_tokens)

        validations = conf.get("validations", [])
        for validation in validations:
            try:
                current_validation = validation["validation"]
                self.__current_action = self.__validations[current_validation]
            except KeyError as e:
                logging.error(
                    f"Validation not found: {current_validation}. Please add the validation to the registry and restart the service")
                raise FieldMapperException(f"Validation not found: {current_validation}.")
            self.__current_action_params = validation.get("params", {})
            path_tokens = validation["path"].split(".")
            value = self.__parse_payload(payload, path_tokens, extract_only=True)
            try:
                self.__parse_payload(payload, path_tokens)
            except ValidationFailedException as e:
                error_dct["validations"].append(f"{current_validation} failed for key: {validation['path']}")

        annotations = conf.get("annotations", [])
        assigned_annotations = set()
        for annotation in annotations:
            try:
                current_annotation = annotation["annotation"]
                self.__current_action = self.__annotations[current_annotation]
            except KeyError as e:
                logging.error(f"Annotation not found: {current_annotation}. Please add the annotation to the registry and restart the service")
                raise FieldMapperException(f"Annotation not found: {current_annotation}.")
            self.__current_action_params = annotation.get("params", {})
            path_tokens = annotation["path"].split(".")
            value = self.__parse_payload(payload, path_tokens, extract_only=True)
            annotation_value = self.__current_action(value, **self.__current_action_params)
            if annotation_value:
                assigned_annotations.add(annotation_value)
        payload["annotations"] = list(assigned_annotations)

        return payload
