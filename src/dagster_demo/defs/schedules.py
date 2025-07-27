import dagster as dg
from .jobs import jaffle_daily_job

jaffle_update_schedule = dg.ScheduleDefinition(
    job=jaffle_daily_job,
    cron_schedule="0 5 * * *",
)
