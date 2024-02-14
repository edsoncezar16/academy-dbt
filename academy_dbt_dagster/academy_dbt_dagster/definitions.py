import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_meltano import MeltanoResource

from .assets import indicium_ae_certification_dbt_assets
from .constants import dbt_project_dir
from .schedules import dbt_schedules
from .meltano_dagster_utils import meltano_dagster_factory
from .constants import meltano_project_dir

meltano_assets, meltano_jobs, meltano_schedules = meltano_dagster_factory(
    MeltanoResource(project_dir=meltano_project_dir)
)

defs = Definitions(
    assets=[indicium_ae_certification_dbt_assets] + meltano_assets,
    schedules=dbt_schedules + meltano_schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)
