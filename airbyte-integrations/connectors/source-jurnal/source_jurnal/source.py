#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import decimal
import json
import logging
import os
from abc import ABC
from glob import glob
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from requests.exceptions import HTTPError

logger = logging.getLogger("airbyte")

class JurnalStream(HttpStream, ABC):
    url_base = "https://api.jurnal.id/core/api/v1/"

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.apikey = config['apikey']
        self.schema_map = self.generate_schema_map()

    def generate_schema_map(self) -> Mapping[str, Any]:
        schemas_dir = os.path.join(os.getcwd(), "source_jurnal/schemas")
        schema_file_path_template = os.path.join(schemas_dir , "*.json")
        schemas = {}
        for schema_file_path in glob(schema_file_path_template):
            filename = Path(schema_file_path).stem
            schema_file = open(schema_file_path)
            schema = json.load(schema_file)
            schemas[filename] = schema
        return schemas
    
    def get_schema_property_keys(self, schema) -> List[str]:
        return list(schema["properties"].keys())

    def remove_redundant_properties(self, records: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
        filtered_records = []
        schema = super().get_json_schema()
        schema_property_keys = self.get_schema_property_keys(schema)
        for record in records:
            filtered_records.append({key: record[key] for key in schema_property_keys})
        return filtered_records

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'apikey': self.apikey}
        
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}
    
    def path (
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return None
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

class Accounts(JurnalStream):
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config=config, **kwargs)
        self.schema_name = __class__.__name__.lower()

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "accounts"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        accounts = response.json(parse_float=decimal.Decimal)["accounts"]
        accounts = self.remove_redundant_properties(accounts)
        for account in accounts:
            yield account

class SourceJurnal(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return JurnalStream(config=config).check_connection(logger, config)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Accounts(config=config)]

