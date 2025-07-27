from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource
from dagster_dlt import DagsterDltResource
from dagster_demo.defs.project import dbt_project

import dagster as dg


database_resource = BigQueryResource(
    project=dg.EnvVar("GCP_PROJECT_ID"),
    location="us-central1",
    gcp_credentials=dg.EnvVar("GOOGLE_APPLICATION_CREDENTIALS")
)

dlt_resource = DagsterDltResource()

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

defs = dg.Definitions(
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "dlt": dlt_resource,
    },
)
