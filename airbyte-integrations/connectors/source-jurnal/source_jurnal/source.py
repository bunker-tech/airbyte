#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


class JurnalStream(HttpStream, ABC):
    url_base = "https://api.jurnal.id/core/api/v1/"

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.apikey = config['apikey']

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
        self.accounts = []

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "accounts"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.accounts = [response.json()]
        return self.accounts

class SourceJurnal(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger = logging.getLogger("airbyte")
        return JurnalStream(config=config).check_connection(logger, config)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Accounts(config=config)]
