# runestone-scheduled-job

This is intended to be a cron job that will send specific events from Runestone Server to a Caliper Endpoint.

## Start docker

Run `docker-compose up`

## Description

This task is intended to be run as a scheduled job outside of caliper. You need to define all of the environment variables in .env.sample to point to your Postgres database and Caliper endpoint.

This job will query through all of the events in the Runestone useinfo table and process them into Caliper events.

Currently the only event(s) that are supported are
event_type "page" / act_type "view" -> ViewEvent

The status of the job is written to the table cron_run_info -> cron_job ->runestone_caliper_job table in the database. This table value of the last success run is also used for the next run. 

There are some other configuration options such as BATCH_SIZE which is how many values are sent at a time and also FIRST_EVENT_TIME which is the first time to use for event sending (incase you want to try to re-send old events)
