import yaml

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    AssetOut,
    OpExecutionContext,
    ConfigurableResource,
    AssetSelection,
    define_asset_job,
    multi_asset,
)
from dagster_meltano import MeltanoResource
from typing import Mapping, Any




def get_catalog(plugin_name: str, meltano: MeltanoResource) -> list[Mapping[str, Any]]:
    """
    Returns the catalog of all streams for a given plugin from the Meltano project.
    """
    try:
        catalog = meltano.load_json_from_cli(["invoke", "--dump=catalog", plugin_name])
        return catalog.get("streams", [])
    except ValueError as e:
        print(f"Error generating catalog for plugin '{plugin_name}': {e.stderr}")
        return []


def get_selected_streams(meltano: MeltanoResource) -> Mapping[str, list[str]]:
    """
    Generates a mapping of a plugin to its selected streams across all plugins in the Meltano project.
    """
    with open(meltano.project_dir + "/meltano.yml", "r") as file:
        meltano_config = yaml.safe_load(file)

    plugins = meltano_config.get("plugins", {})
    selected_streams = {}

    for plugin_type, plugin_list in plugins.items():
        for plugin in plugin_list:
            plugin_name = plugin.get("name")[4:] # removes the 'tap-' prefix
            if plugin_name:
                catalog = get_catalog(plugin_name)
                selected_streams[plugin_name] = [
                    stream for stream in catalog if stream.get("selected")
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
            name=tap_name,
            resource_defs={'meltano': MeltanoResource},
            compute_kind="meltano",
            group_name=tap_name,
            outs={
            stream: AssetOut()
            for stream
            in tap_streams
            }
        )
        def compute(context: OpExecutionContext, meltano: MeltanoResource):
            command = f"run tap-{context.op.name} target-postgres"
            meltano.execute_command(f"{command}", dict(), context.log)
            return tuple([None for _ in context.selected_output_names])
        
        multi_assets.append(compute)

        asset_job = define_asset_job(f"{tap_name}_assets", AssetSelection.groups(tap_name))

        basic_schedule = ScheduleDefinition(
            job=asset_job, 
            cron_schedule="0 0 * * 1-5", 
            default_status=DefaultScheduleStatus.RUNNING
        )

        jobs.append(asset_job)
        schedules.append(basic_schedule)

    return multi_assets, jobs, schedules