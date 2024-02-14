import yaml
import json

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    AssetOut,
    OpExecutionContext,
    AssetSelection,
    define_asset_job,
    multi_asset,
)
from dagster_meltano import MeltanoResource
from dagster_meltano.utils import generate_dagster_name
from typing import Mapping, Any
from .constants import meltano_project_dir


def get_catalog(tap_name: str, meltano: MeltanoResource) -> list[Mapping[str, Any]]:
    """
    Returns the catalog of all streams for a given tap from the Meltano project.
    tap_name is the name after the 'tap-' prefix, e.g, for tap-postgres we have tap_name='postgres'.
    """
    try:
        catalog = json.loads(meltano.execute_command(f"invoke --dump=catalog tap-{tap_name}", env={}))
        return catalog.get("streams")
    except Exception as e:
        print(f"Error generating catalog for plugin 'tap-{tap_name}'. {e}")
        return []


def get_selected_streams(meltano: MeltanoResource) -> dict[str, list[str]]:
    """
    Generates a mapping of a plugin to its selected streams across all plugins in the Meltano project.
    """
    with open(meltano.project_dir + "/meltano.yml", "r") as file:
        meltano_config = yaml.safe_load(file)

    plugins = meltano_config.get("plugins", {})
    taps = plugins.get("extractors", {})
    selected_streams = {}

    for tap in taps:
        full_tap_name = tap.get("name")
        if full_tap_name:
            tap_name = full_tap_name[4:]  # removes the 'tap-' prefix
            catalog = get_catalog(tap_name, meltano)
            selected_streams[tap_name] = [
                stream.get("tap_stream_id")
                for stream in catalog
                if stream.get("selected")
            ]

    return selected_streams


def meltano_dagster_factory(meltano: MeltanoResource) -> list[Any]:
    """
    Generates all dagster assets corresponding to a meltano stream, besides jobs with a
    predefined schedule to orchestrate these assets.
    """
    multi_assets = []
    jobs = []
    schedules = []

    for tap_name, tap_streams in get_selected_streams(meltano).items():

        @multi_asset(
            name=generate_dagster_name(tap_name),
            compute_kind="meltano",
            resource_defs={"meltano": MeltanoResource(project_dir=meltano_project_dir)},
            group_name=generate_dagster_name(tap_name),
            outs={
                generate_dagster_name(stream_name): AssetOut(
                    key=generate_dagster_name(stream_name)
                )
                for stream_name in tap_streams
            },
        )
        def compute(context: OpExecutionContext):
            command = f"run tap-{context.op.name} target-bigquery"
            meltano = context.resources.get("meltano")
            if meltano:
                meltano.execute_command(f"{command}", dict(), context.log)
                return tuple([None for _ in context.selected_output_names])

        multi_assets.append(compute)

        asset_job = define_asset_job(
            f"{tap_name}_assets", AssetSelection.groups(tap_name)
        )

        basic_schedule = ScheduleDefinition(
            job=asset_job,
            cron_schedule="0 0 * * 1-5",
            default_status=DefaultScheduleStatus.RUNNING,
        )

        jobs.append(asset_job)
        schedules.append(basic_schedule)

    return multi_assets, jobs, schedules
