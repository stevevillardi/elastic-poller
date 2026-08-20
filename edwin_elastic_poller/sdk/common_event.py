# SPDX-FileCopyrightText: 2023, 2024 LogicMonitor, Inc.
#
# SPDX-License-Identifier: LicenseRef-All-rights-reserved

"""Module providing representation of, and logic for, an Edwin common event."""

import datetime
import enum
import logging
import pathlib
import uuid
import typing

import dateutil
import dateutil.parser
import jsonpath_ng
import pydantic
import pydantic.validators
import yaml

_logger = logging.getLogger("edwin_elastic_poller.sdk.common_event")

class StrEnum(str, enum.Enum):
    """Used to create StrEnum class for compatibility with Python 3.9."""
    # pylint: disable-next=unnecessary-pass
    pass

class _CefKey(StrEnum):
    """Class to store enums of CEF keys."""
    EVENT_CI = "event_ci"
    EVENT_OBJECT = "event_object"
    EVENT_SOURCE = "event_source"
    EVENT_NAME = "event_name"
    EVENT_DESCRIPTION = "event_description"
    EVENT_SEVERITY = "event_severity"
    EVENT_TIME = "event_time"
    EVENT_DETAILS = "event_details"
    SOURCE_RECORD = "source_record"
    EVENT_ID = "event_id"
    EVENT_CI_LINK = "event_ci_link"
    EVENT_NAME_LINK = "event_name_link"
    EVENT_SOURCE_ID = "event_source_id"
    EVENT_SOURCE_ID_LINK = "event_source_id_link"
#    EVENT_ENRICHMENTS = "event_enrichments"
    CLASS = "class"
    VERSION = "version"
    EVENT_DOMAIN = "event_domain"

class _CefSeverity(enum.IntEnum):
    """Class to store enums of CEF severity."""
    CRITICAL = 5
    MAJOR = 4
    MINOR = 3
    WARNING = 2
    INDETERMINATE = 1
    CLEAR = 0

class _CefMappingKey(StrEnum):
    """Class to store enums for mapping keys."""
    MAPPINGS = "mappings"
    TIMESTAMPS = "timestamps"
    TRANSFORMS = "transforms"
    DEFAULTS = "defaults"

class _CefTimestampTypes(StrEnum):
    """Class to store enums of timestamp type."""
    UNIX = "unix"
    DATETIME = "datetime"

class _JsonpathMappings(pydantic.BaseModel, extra="forbid", strict=True):
    """Pydantic model for validating user-supplied jsonpath mappings.
    These mappings will be used to retrieve values from the original
    data object.
    """
    event_ci: typing.List[str]
    event_object: typing.List[str]
    event_source: typing.List[str]
    event_name: typing.List[str]
    event_description: typing.List[str]
    event_severity: typing.List[str]
    # event_id and event_time are optional as we can generate them if needed
    event_id: typing.Optional[typing.List[str]] = None
    event_time: typing.Optional[typing.List[str]] = None
    # event_domain is only used for domain/tenant environments
    event_domain: typing.Optional[typing.List[str]]
    # The following fields are optional, so do not need jsonpath mappings
    event_details: typing.Optional[typing.List[str]]
    event_ci_link: typing.Optional[typing.List[str]]
    event_name_link: typing.Optional[typing.List[str]]
    event_source_id: typing.Optional[typing.List[str]]
    event_source_id_link: typing.Optional[typing.List[str]]
  #  event_enrichments: typing.Optional[typing.List[str]]

class _CefDefaults(pydantic.BaseModel, extra="forbid", strict=True):
    """Pydantic model for validating user-supplied default values."""
    event_ci: str
    event_object: str
    event_source: str
    event_name: str
    event_description: str
    # Allow int or str values that are ints e.g. "4"
    event_severity: int = pydantic.Field(strict=False)
    # event_domain is only used for domain/tenant environments
    event_domain: typing.Optional[str]
    # The following fields are optional, so do not need defaults
    event_details: typing.Optional[str]
    event_ci_link: typing.Optional[pydantic.HttpUrl]
    event_name_link: typing.Optional[pydantic.HttpUrl]
    event_source_id: typing.Optional[str]
    event_source_id_link: typing.Optional[pydantic.HttpUrl]
 #   event_enrichments: typing.Optional[str]

    @pydantic.field_validator("event_severity")
    @classmethod
    def validate_severity(cls, value: int) -> int:
        """Validate the default severity is a valid severity.
        :param value: The value provided in mapping.
        :raises ValueError: Value provided is not a valid severity.
        :returns: The validated value.
        """
        if value not in _CefSeverity.__members__.values():
            raise ValueError(f"Default severity must be a valid severity. "
                f"Value provided: {value}. "
                f"Valid values: {[(s.name, s.value) for s in _CefSeverity]}")
        return value

class _TimestampMappings(pydantic.BaseModel, extra="forbid", strict=True):
    """Pydantic model for validating user-supplied timestamp mappings."""
    type: str
    day_first: bool
    year_first: bool
    offset: typing.Optional[typing.Dict[str, int]] = pydantic.Field(
        default=None)

    @pydantic.field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Validate type.
        :param value: The value provided in mapping.
        :raises ValueError: Value provided is not a valid type.
        :returns: The validated value.
        """
        if value not in _CefTimestampTypes.__members__.values():
            raise ValueError(f"Timestamp type is invalid. "
                f"Type provided: {value}. "
                f"Valid types: {[(s.value) for s in _CefTimestampTypes]}")
        return value

    @pydantic.model_validator(mode="after")
    def check_day_and_year_first(self) -> "_TimestampMappings":
        """Validate that only one of day_first or year_first is True, if type
        is datetime. If type is unix, those options are ignored.
        :raises ValueError: Both day_first and year_first are True
        :returns: Model.
        """
        if(self.type == _CefTimestampTypes.DATETIME.value and
        all([self.day_first, self.year_first])):
            raise ValueError("day_first and year_first both cannot be set "
                "to True")
        return self

class _CefTransforms(pydantic.BaseModel, extra="allow", strict=True):
    """Pydantic model for validating user-supplied transform config.
    Extra attributes are allowed because we may have more than just
    severity to transform.
    """
    event_severity: typing.Dict[str, typing.List[typing.Union[str, int]]]

    @pydantic.field_validator("event_severity")
    @classmethod
    def validate_severity_transform(
        cls,
        value: typing.Dict[str, typing.List[typing.Union[str, int]]]
    ) -> typing.Dict[str, typing.List[typing.Union[str, int]]]:
        """Validate the severity transform mapping.
        :param value: The value provided in mapping.
        :raises KeyError: Missing a key, or an extra key has been found.
        :returns: The validated value.
        """
        key_list = list(k.upper() for k in value.keys())
        severities = list(_CefSeverity.__members__)
        if len(set(key_list).symmetric_difference(severities)) != 0:
            raise KeyError(f"Mismatch of keys in transform.event_severity. "
                f"Provided keys: {key_list}. "
                f"Expected keys: {severities}")
        return value

class CommonEvent:
    """Class representing an Edwin Common Event. Also provides logic for mapping
    an external event format to the Common Event Format (CEF)."""
    _VALUE = typing.Union[str, int, bool]

    _CEF_MANDATORY_LIST: typing.List = [
        _CefKey.EVENT_CI.value,
        _CefKey.EVENT_OBJECT.value,
        _CefKey.EVENT_SOURCE.value,
        _CefKey.EVENT_NAME.value,
        _CefKey.EVENT_DESCRIPTION.value,
        _CefKey.EVENT_SEVERITY.value,
    ]

    _CEF_AUTO_GENERATE_LIST: typing.List = [
        _CefKey.EVENT_TIME.value,
        _CefKey.EVENT_ID.value,
    ]

    _CEF_NON_MANDATORY_LIST: typing.List = [
        _CefKey.EVENT_DETAILS.value,
        _CefKey.EVENT_CI_LINK.value,
        _CefKey.EVENT_NAME_LINK.value,
        _CefKey.EVENT_SOURCE_ID.value,
        _CefKey.EVENT_SOURCE_ID_LINK.value,
#        _CefKey.EVENT_ENRICHMENTS.value,
    ]

    _FILE_DIR: str = "src/logicmonitor/edwin/common_event_integration_sdk/"

    @classmethod
    def new_from_file(
        cls,
        mapping_file_name: str,
        mapping_file_path: typing.Optional[str] = None,
        original_record: typing.Optional[typing.Dict[str, _VALUE]] = None,
    ) -> "CommonEvent":
        """Class method to start new CommonEvent using config files.
        :param mapping_file_name: Name of mapping config file to use.
        :param mapping_file_path: File path of config file to use (optional).
        :param original_record: Original record to convert to CEF (optional).
        :raises FileNotFoundError: Cannot find the config file using the name
        (and filepath, if passed) given.
        :raises ValueError: Missing one or more of the required mapping
        sections in mapping config file.
        :returns: Instance of CommonEvent started using provided files.
        """
        # print("Mapping File : " + mapping_file_name)
        _file_path = (mapping_file_path if mapping_file_path is not None
                    else cls._FILE_DIR)
        _mfp = pathlib.Path(_file_path).joinpath(mapping_file_name)
        try:
            _mapping_yaml: typing.Dict = yaml.safe_load(
                _mfp.read_text(encoding = "utf-8"))
            _logger.debug(
                "Mappings read from file: %s\nFull path: %s",
                mapping_file_name,
                _mfp,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Unable to find mapping config file\n"
                                    f"File name: \"{mapping_file_name}\"\n"
                                    f"Path: \"{_mfp}\"") from e
        return cls.new_from_param(
            mapping_dict=_mapping_yaml,
            original_record=original_record,
        )

    @classmethod
    def new_from_param(
        cls,
        mapping_dict: typing.Dict,
        original_record: typing.Optional[typing.Dict[str, _VALUE]] = None,
    ) -> "CommonEvent":
        """Class method to start new CommonEvent using params.
        :param mapping_dict: Dict containing mappings with JsonPath, severity
        conversion mappings and default values.
        :param original_record: Original record to convert to CEF (optional).
        :returns: Instance of CommonEvent started using provided params.
        """
        mappings: "_JsonpathMappings" = _JsonpathMappings.model_validate(
            obj = mapping_dict.get(_CefMappingKey.MAPPINGS.value))
        defaults: "_CefDefaults" = _CefDefaults.model_validate(
            obj = mapping_dict.get(_CefMappingKey.DEFAULTS.value))
        timestamps: "_TimestampMappings" = _TimestampMappings.model_validate(
            obj = mapping_dict.get(_CefMappingKey.TIMESTAMPS.value))
        transforms: "_CefTransforms" = _CefTransforms.model_validate(
            obj = mapping_dict.get(_CefMappingKey.TRANSFORMS.value))
        
        return cls(
            mapping_dict = mapping_dict,
            mappings = mappings,
            defaults = defaults,
            transforms = transforms,
            timestamps = timestamps,
            original_record = original_record,
        )

    def __init__(
        self,
        mapping_dict: typing.Dict,
        mappings: "_JsonpathMappings",
        defaults: "_CefDefaults",
        timestamps: "_TimestampMappings",
        transforms: "_CefTransforms",
        original_record: typing.Optional[typing.Dict[str, _VALUE]] = None,
    ) -> None:
        """
        :param mapping_dict: Dict containing JsonPath mappings, severity
        mappings and default value mappings.
        :param mappings: jsonpath mappings provided in config that have been
        validated by pydantic.
        :param defaults: Default values provided in config that have been
        validated by pydantic.
        :param timestamps: Timestamp details provided in config that have been
        validated by pydantic.
        :param transforms: Transform mappings provided in config that have been
        validated by pydantic.
        :param original_record: Original record to convert to CEF (optional).
        """
        _logger.debug(
            "Initializing CommonEvent mapping_fields=%s has_source_record=%s",
            list(mapping_dict),
            original_record is not None,
        )
        self.mappings: "_JsonpathMappings" = mappings
        self.timestamp_mappings: "_TimestampMappings" = timestamps
        self.transform_mappings: "_CefTransforms" = transforms
        self.default_mapping_values: "_CefDefaults" = defaults
        self.cef_dict: typing.Dict[str, self._VALUE] = {
            _CefKey.EVENT_CI.value: None,
            _CefKey.EVENT_OBJECT.value: None,
            _CefKey.EVENT_SOURCE.value: None,
            _CefKey.EVENT_NAME.value: None,
            _CefKey.EVENT_DESCRIPTION.value: None,
            _CefKey.EVENT_SEVERITY.value: None,
            _CefKey.EVENT_TIME.value: None,
            _CefKey.EVENT_ID.value: None,
            _CefKey.SOURCE_RECORD.value: {},
            # Class and version are hardcoded via the SDK
            _CefKey.CLASS.value: "event",
            _CefKey.VERSION.value: "1.1",
            # event_domain is required to be present but can be empty
            _CefKey.EVENT_DOMAIN.value: "",
            # The following fields are not required so set to empty string
            _CefKey.EVENT_DETAILS.value: "",
            _CefKey.EVENT_CI_LINK.value: "",
            _CefKey.EVENT_NAME_LINK.value: "",
            _CefKey.EVENT_SOURCE_ID.value: "",
            _CefKey.EVENT_SOURCE_ID_LINK.value: "",
 #           _CefKey.EVENT_ENRICHMENTS.value: "",
        }
        self.enrichment_dict: typing.Dict[str, str] = {}
        if original_record is not None:
            self.set_field(_CefKey.SOURCE_RECORD.value, original_record)
            self._convert_original_record_to_cef(original_record)

    def __str__(self) -> str:
        """Return event_id and event_time for human readable logging.
        :returns: event_id and event_time concatenated into one string.
        """
        return f"{self.cef_dict['event_id']} - {self.cef_dict['event_time']}"

    def __repr__(self) -> str:
        """Return entire cef and enrichment dict as string.
        :returns: entire CEF as a string.
        """
        return str(self.get_cef())

    def _convert_original_record_to_cef(
            self, original_record: typing.Dict[str, _VALUE]) -> None:
        """Convert original record to CEF.
        :param original_record: Original record to convert to CEF.
        """
        
        #print(original_record)
        allowable_empty_jsonpath_field_list = self._CEF_NON_MANDATORY_LIST.copy()
        allowable_empty_jsonpath_field_list.extend(self._CEF_AUTO_GENERATE_LIST)
        for field_name, json_path_list in iter(self.mappings):
            try:
                _logger.debug("field_name: %s, json_path_list: %s",
                    field_name, json_path_list)
                value = None
                if((field_name in allowable_empty_jsonpath_field_list and
                    json_path_list is not None)
                    or field_name in self._CEF_MANDATORY_LIST):
                    for json_path in json_path_list:
                        jsonpath_expr = jsonpath_ng.parse(json_path)
                        match_list = jsonpath_expr.find(original_record)
                        if match_list:
                            break
                    try:
                        value = match_list[0].value
                        transform =  getattr(
                            self.transform_mappings,
                            field_name,
                            None
                        )
                        if match_list and field_name == _CefKey.EVENT_TIME.value:
                            value = self._convert_timestamp(match_list[0].value)
                        if match_list and transform is not None:
                            value = self._perform_transform(
                                field_name,
                                transform,
                                match_list[0].value,
                            )
                    except IndexError:
                        # IndexError raised if the original record does not have
                        # the attribute that is being searched for by the jsonpath
                        _logger.debug(
                            "Unable to find matching attribute in "
                            "original_record for field %s using jsonpath(s) %s",
                            field_name,
                            json_path_list,
                        )
                self.set_field(field_name, value)
            except Exception:
                _logger.exception(
                    "Error converting source record field=%s", field_name
                )
    def get_cef(self) -> typing.Dict:
        """Getter for final cef object.
        :returns: entire CEF as a dict.
        """
        self._final_cef_check()
        final_cef = {"cef": self.cef_dict, "enrichments": self.enrichment_dict}
        _logger.debug(
            "Final CommonEvent generated cef_fields=%s enrichment_fields=%s",
            list(self.cef_dict),
            list(self.enrichment_dict),
        )
        return final_cef

    def _final_cef_check(self) -> None:
        """Final check for empty mandatory values."""
        fields_to_not_check = [
            _CefKey.SOURCE_RECORD.value,
            _CefKey.CLASS.value,
            _CefKey.VERSION.value,
            _CefKey.EVENT_DOMAIN.value,
        ]
        fields_to_not_check.extend(self._CEF_NON_MANDATORY_LIST)
        for field_name, field_value in list(self.cef_dict.items()):
            if(field_name not in fields_to_not_check and
            (field_value is None or not field_value)):
                value = self._set_empty_mandatory_field(field_name)
                self.cef_dict[field_name] = value
            if(field_name in self._CEF_NON_MANDATORY_LIST and
            field_value == ""):
                del self.cef_dict[field_name]

    def set_fields(self, value_dict: typing.Dict[str, _VALUE]) -> None:
        """Set value field(s) using a dictionary containing the key: pair if
        the key exists and has a value if mandatory.
        :param value_dict: dictionary of key: values to add to CEF.
        """
        for field_name, field_value in value_dict.items():
            self.set_field(field_name, field_value)

    def set_field(self, field_name: str, field_value: _VALUE) -> None:
        """Set value for a single field using a name if it exists as a key and
        has a value if mandatory.
        :param field_name: name of the field to set.
        :param field_value: value of the field to be set.
        """
        if field_name in self.cef_dict:
            if not self._check_mandatory_field(field_name, field_value):
                _logger.debug(
                    "Value of type %s is not valid for %s field; "
                    "using configured default",
                    type(field_value),
                    field_name,
                )
                field_value = self._set_empty_mandatory_field(field_name)
            if field_name == _CefKey.EVENT_SEVERITY.value:
                self.cef_dict[field_name] = int(field_value)
            elif field_name == _CefKey.SOURCE_RECORD.value:
                self.cef_dict[field_name] = field_value
            elif((field_name in self._CEF_NON_MANDATORY_LIST or
                field_name == _CefKey.EVENT_DOMAIN.value)and
                field_value is None):
                # Non-mandatory fields and event_domain can be None
                pass
            else:
                self.cef_dict[field_name] = str(field_value)
        else:
            _logger.info("Field %s is not required by the CEF", field_name)

    def _check_mandatory_field(
            self, field_name: str, field_value: _VALUE) -> bool:
        """Check for a value for a mandatory field.
        :param field_name: name of the field to check.
        :param field_value: value of the field to check.
        :returns: boolean indicating if the field_value is valid.
        """
        if field_name == _CefKey.EVENT_ID.value:
            return self._is_valid_uuid(field_value)
        if field_name == _CefKey.EVENT_TIME.value:
            return self._is_valid_timestamp(field_value)
        if field_name == _CefKey.EVENT_SEVERITY.value:
            return self._is_valid_severity(field_value)
        if(field_name in self._CEF_MANDATORY_LIST
           and (field_value is None or field_value == ""
           or not isinstance(field_value, str))):
            return False
        return True

    def _is_valid_uuid(self, value: str) -> bool:
        """Checks if value is a valid uuid.
        :param value: value to check.
        :returns: boolean indicating if value is a valid uuid.
        """
        try:
            uuid.UUID(value)
        except (TypeError, ValueError):
            return False
        return True

    def _is_valid_timestamp(self, value: str) -> bool:
        """Checks if value is valid timestamp in correct format.
        :param value: value to check.
        :raises ValueError: Raised when unable to parse timestamp.
        :returns: boolean indicating if value is correctly formatted timestamp.
        """
        if value is None:
            return False
        dateutil.parser.isoparse(value)
        return True

    def _is_valid_severity(self, value: typing.Union[str, int]) -> bool:
        """Checks if the value is a valid severity.
        :param value: value to check.
        :returns: boolean indicating if value is a valid severity.
        """
        try:
            parsed_value = int(value)
        except (ValueError, TypeError):
            # ValueError if value is not a int-like value
            # TypeError if value is None
            return False
        if parsed_value in iter(_CefSeverity):
            return True
        return False

    def _perform_transform(
            self,
            field_name: str,
            transform_dict: typing.Dict[str, typing.List[typing.Union[str, int]]],
            value: typing.Union[str, int],
    ) ->  typing.Union[str, int, None]:
        """Performs transform for a given field if provided in mappings.
        :param field_name: the name of the field being transformed.
        :param transform_dict: dict containing key to transform into from the
        list of potential values.
        :param value: value that needs to be transformed.
        :returns: value of transform.
        """
        for key, value_list in transform_dict.items():
            if value in value_list:
                if field_name == _CefKey.EVENT_SEVERITY.value:
                    return self._convert_severity(key)
                return key
        return None

    def _convert_severity(self, value: str) -> typing.Union[int, None]:
        """Converts severity into known CEF format.
        :param value: key of severity parsed in transform.
        :returns: int value of severity. None is returned if the value is not
        given in mappings.
        """
        if value.upper() in _CefSeverity.__members__:
            return _CefSeverity[value.upper()].value
        return None

    def _convert_timestamp(self, value: str) -> typing.Union[str, None]:
        """Converts a given value (that should be a timestamp of some format)
        into the CEF timestamp.
        :param value: original timestamp from data source to convert.
        :raises ValueError: raised when unable to parse value into timestamp.
        :returns: str of converted timestamp in ISO format.
        """
        try:
            if self.timestamp_mappings.type == _CefTimestampTypes.DATETIME.value:
                converted_timestamp = dateutil.parser.parse(
                    timestr = value,
                    dayfirst = self.timestamp_mappings.day_first,
                    yearfirst = self.timestamp_mappings.year_first,
                    tzinfos = self.timestamp_mappings.offset
                )
                val = datetime.datetime.fromtimestamp(
                    converted_timestamp.timestamp(),
                    datetime.timezone.utc
                    ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-1]
            else:
                val = datetime.datetime.fromtimestamp(
                    float(value),
                    datetime.timezone.utc
                    ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-1]
            return f"{val}Z"
        except dateutil.parser.ParserError:
            _logger.error("Unable to parse %s into a timestamp", value)
            raise ValueError(f"Unable to parse {value} "
                            "into a timestamp") from None

    def _set_empty_mandatory_field(
            self, field_name: str) -> typing.Union[str, int]:
        """Set a value for an empty mandatory field using configured defaults.
        :param field_name: name of field to set default value for.
        :returns: default value for given field_name.
        """
        if field_name == _CefKey.EVENT_ID.value:
            return str(uuid.uuid1())
        if field_name == _CefKey.EVENT_TIME.value:
            val = datetime.datetime.now(
                datetime.timezone.utc
                ).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-1]
            return f"{val}Z"
        default_val = getattr(self.default_mapping_values, field_name)
        if field_name != _CefKey.EVENT_SEVERITY.value:
            default_val = str(default_val)
        return default_val

    def set_enrichment_values(
            self, enrichment_dict: typing.Dict[str, _VALUE]) -> None:
        """Set enrichment field(s) using a dictionary containing a key:value
        pair and converting the value to str.
        :param enrichment_dict: dictionary of key: values to add as enrichments
        in CEF.
        """
        for field_name, field_value in enrichment_dict.items():
            self.set_enrichment_value(field_name, field_value)

    def set_enrichment_value(
            self, enrichment_field: str, enrichment_value: _VALUE) -> None:
        """Set value for a single enrichment field using name and converting
        the value to str.
        :param enrichment_field: name of the field to set as an enrichment
        in CEF.
        :param enrichment_value: value of the field to be set as an enrichment
        in CEF.
        """
        self.enrichment_dict[enrichment_field] = str(enrichment_value)
