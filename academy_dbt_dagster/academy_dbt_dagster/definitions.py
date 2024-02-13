import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_meltano import MeltanoResource

from .assets import indicium_ae_certification_dbt_assets
from .constants import dbt_project_dir, meltano_project_dir
from .schedules import dbt_schedules
from .meltano_dagster_utils import meltano_dagster_factory

meltano_assets, meltano_jobs, meltano_schedules = meltano_dagster_factory()

defs = Definitions(
    assets=[indicium_ae_certification_dbt_assets],
    schedules=dbt_schedules + meltano_schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "meltano": MeltanoResource(project_dir=meltano_project_dir),
    },
)
