import dagster as dg

start_date = "2024-09-01"

daily_partition = dg.DailyPartitionsDefinition(
    start_date=start_date
)
