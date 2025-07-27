from dagster import AssetExecutionContext, Definitions, AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt import pipeline
import dlt
import os
import csv


class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the dlt resource name."""
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            key=AssetKey(f"{data.resource.name}"),
            deps=None
        )

JAFFLE_PATH = "/Users/mac/Documents/working/vieon/dagster-demo/data/jaffle-data"

def make_dlt_asset(filename):
    file_path = os.path.join(JAFFLE_PATH, filename)
    asset_key = filename.removesuffix(".csv")

    @dlt.resource(name=asset_key, write_disposition='replace')
    def load_csv():
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        yield data

    @dlt.source
    def source_name():
        return load_csv

    @dlt_assets(
        dlt_source=source_name(),
        dlt_pipeline=pipeline(
            pipeline_name=f"JAFFLE_SHOP_RAW_{asset_key}",
            dataset_name="JAFFLE_SHOP_RAW",
            destination="bigquery"
        ),
        name=asset_key,
        group_name="raw",
        dagster_dlt_translator=CustomDagsterDltTranslator()
    )
    def asset_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(
            context=context,
            dlt_source=source_name(),
            dlt_pipeline=pipeline(
                pipeline_name=f"JAFFLE_SHOP_RAW_{asset_key}",
                dataset_name="JAFFLE_SHOP_RAW",
                destination="bigquery"
            ),
        )
    return asset_fn

# Collect all asset functions
assets = []
for filename in os.listdir(JAFFLE_PATH):
    file_path = os.path.join(JAFFLE_PATH, filename)
    if filename.endswith(".csv") and os.path.isfile(file_path):
        assets.append(make_dlt_asset(filename))

# Register assets with Dagster
defs = Definitions(assets=assets)
