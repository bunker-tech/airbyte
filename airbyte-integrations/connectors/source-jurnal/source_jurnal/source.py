#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Tuple

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import *


class SourceJurnal(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        return JurnalStream(config=config).check_connection()

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Accounts(config=config),
            JournalEntries(config=config),
            Vendors(config=config),
            Expenses(config=config),
            PurchaseInvoices(config=config),
        ]
