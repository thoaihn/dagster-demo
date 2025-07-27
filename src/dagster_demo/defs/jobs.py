import dagster as dg
from .partitions import daily_partition



jaffle_daily_job = dg.define_asset_job(
    name="jaffle_daily_job",
    partitions_def=daily_partition,
    selection=dg.AssetSelection.all()
)
