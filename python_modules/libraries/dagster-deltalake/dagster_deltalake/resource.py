from typing import Optional, Union

from dagster import ConfigurableResource
from deltalake import DeltaTable
from pydantic import Field

from .config import AzureConfig, ClientConfig, GcsConfig, LocalConfig, S3Config


class DeltaTableResource(ConfigurableResource):
    """Resource for interacting with a Delta table.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_deltalake import DeltaTableResource, LocalConfig

            @asset
            def my_table(delta_table: DeltaTableResource):
                df = delta_table.load().to_pandas()

            defs = Definitions(
                assets=[my_table],
                resources={
                    "delta_table": DeltaTableResource(
                        url="/path/to/table",
                        storage_options=LocalConfig()
                    )
                }
            )

    """

    url: str

    storage_options: Union[AzureConfig, S3Config, LocalConfig, GcsConfig] = Field(
        discriminator="provider"
    )

    client_options: Optional[ClientConfig] = Field(
        default=None, description="Additional configuration passed to http client."
    )

    def load(self) -> DeltaTable:
        storage_options = self.storage_options.dict() if self.storage_options else {}
        client_options = self.client_options.dict() if self.client_options else {}
        options = {**storage_options, **client_options}
        table = DeltaTable(table_uri=self.url, storage_options=options)
        return table
