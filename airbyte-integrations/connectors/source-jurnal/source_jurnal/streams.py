#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import decimal
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources.streams.http import HttpStream


class JurnalStream(HttpStream, ABC):
    url_base = "https://api.jurnal.id/core/api/v1/"

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.apikey = config["apikey"]

    def get_schema_property_keys(self, schema: Mapping[str, Any]) -> List[str]:
        return list(schema["properties"].keys())

    def remove_redundant_properties(self, records: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
        filtered_records = []
        schema = super().get_json_schema()
        schema_property_keys = self.get_schema_property_keys(schema)
        for record in records:
            filtered_record = {key: record[key] for key in schema_property_keys}
            filtered_records.append(filtered_record)
        return filtered_records

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"apikey": self.apikey}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def path(
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
