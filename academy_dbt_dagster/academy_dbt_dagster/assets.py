from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)

from .constants import dbt_manifest_path, meltano_project_dir

from typing import Mapping, Any, Optional
import json
import yaml


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    _settings = DagsterDbtTranslatorSettings(enable_asset_checks=True)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        """
        Sets dagster asset group as dbt model schema.
        """
        return dbt_resource_props["schema"]


@dbt_assets(
    manifest=dbt_manifest_path,
    exclude="resource_type:seed",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    partitions_def=DailyPartitionsDefinition(
        start_date="2011-05-31", end_date="2014-07-01"
    ),
)
def indicium_ae_certification_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
):
    start, end = context.partition_key_range
    dbt_vars = {"min_date": start.isoformat(), "max_date": end.isoformat()}
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(dbt_build_args, context=context).stream()
