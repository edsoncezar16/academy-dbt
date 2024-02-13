from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)

from .constants import dbt_manifest_path

from typing import Mapping, Any, Optional


class CustomDagsterDbtTranslator(
    DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    )
):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        """
        Sets dagster asset group as dbt model schema.
        """
        return dbt_resource_props["schema"]


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def indicium_ae_certification_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
):
    yield from dbt.cli(["build"], context=context).stream()
