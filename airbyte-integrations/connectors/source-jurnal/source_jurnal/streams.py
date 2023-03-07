#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import collections
import decimal
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream


class JurnalStream(HttpStream, ABC):
    url_base = "https://api.jurnal.id/core/api/v1/"

    resource_path = ""
    response_records_key = None
    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.config = config
        self.apikey = config["apikey"]

    def get_schema_property_keys(self, schema: Mapping[str, Any]) -> List[str]:
        return list(schema["properties"].keys())

    def remove_redundant_properties(self, records: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
        formatted_records = []
        schema = super().get_json_schema()
        schema_property_keys = self.get_schema_property_keys(schema)
        for record in records:
            formatted_record = {key: record[key] for key in schema_property_keys}
            formatted_records.append(formatted_record)
        return formatted_records

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
        return self.resource_path

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json(parse_float=decimal.Decimal)
        response_records = response_json[self.response_records_key] if self.response_records_key else response_json
        response_records_is_array = isinstance(response_records, collections.abc.Sequence)
        records = response_records if response_records_is_array else [response_records]
        formatted_records = self.remove_redundant_properties(records)
        for record in formatted_records:
            yield record
        return

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def check_connection(self) -> Tuple[bool, any]:
        try:
            current_active_company_stream = CurrentActiveCompany(config=self.config)
            current_active_company_gen = current_active_company_stream.read_records(sync_mode=SyncMode.full_refresh)
            current_active_company = next(current_active_company_gen)
            current_active_company_id = current_active_company["id"]
            if current_active_company_id:
                return True, None
            return False, "Unable to get current active company"
        except Exception as e:
            return False, e


class CurrentActiveCompany(JurnalStream):
    resource_path = "companies/active"
    response_records_key = "company"
    primary_key = "id"


class Accounts(JurnalStream):
    resource_path = "accounts"
    response_records_key = "accounts"
    primary_key = "id"


class JournalEntries(JurnalStream):
    resource_path = "journal_entries"
    response_records_key = "journal_entries"
    primary_key = "id"


class Vendors(JurnalStream):
    resource_path = "vendors"
    response_records_key = "vendors"
    primary_key = "id"


class Expenses(JurnalStream):
    resource_path = "expenses"
    response_records_key = "expenses"
    primary_key = "id"


class PurchaseInvoices(JurnalStream):
    resource_path = "purchase_invoices"
    response_records_key = "purchase_invoices"
    primary_key = "id"


class SalesInvoices(JurnalStream):
    resource_path = "sales_invoices"
    response_records_key = "sales_invoices"
    primary_key = "id"
