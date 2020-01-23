# -*- coding: future_fstrings -*-
import caliper
import psycopg2
import psycopg2.extras

import requests, json, sys, os, logging
from datetime import datetime, date, time
import os
from dotenv import load_dotenv
import logging
from pprint import pformat

# Configuration is for OpenLRW, obtain bearer token
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, this_dir + "/..")

dotenv = load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

CUR = None
CONN = None

def main():
    global CONN, CUR
    logger.info("Connect to database...")
    CONN = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "runestone"),
        user=os.getenv("DB_USER", "runestone"),
        password=os.getenv("DB_PASS", "runestone"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
    )
    CUR = CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)

    create_runtime_table()
    last_event_time, cron_status = send_caliper_event()
    update_runtime_table(last_event_time, cron_status)


def create_runtime_table():
    """
    Create cron_run_info table if not exists
    """
    try:
        CUR.execute("""
        CREATE TABLE IF NOT EXISTS cron_run_info 
            (id SERIAL PRIMARY KEY,
            cron_job varchar(64) NOT NULL,
            last_run_time timestamp NOT NULL, 
            last_run_status varchar(255) NOT NULL,
            last_sent_event_time timestamp NOT NULL)
            ;""")
    except Exception as err:
        print(err)




def get_last_event_time(cron_job, cron_status):
    """
    Return the last successfully sent event timestamp from the previous job
    """
    try:
        CUR.execute("""
        SELECT last_sent_event_time FROM cron_run_info 
        WHERE cron_job = '{cron_job}' AND last_run_status = '{cron_status}'
        ORDER BY last_sent_event_time DESC LIMIT 1 """.format(cron_job=cron_job, cron_status=cron_status))
        last_event = CUR.fetchone()
        if last_event:
            last_event_time = last_event[0]
        else:
            last_event_time = os.getenv("FIRST_EVENT_TIME", '2019-01-01T00:00:00').replace('T', ' ')

    except Exception as err:
        logger.error(err)
        last_event_time = os.getenv("FIRST_EVENT_TIME", '2019-01-01T00:00:00').replace('T', ' ')
    return last_event_time


def fetch_events(last_event_time, target_events, target_acts):
    """
    Return all the events happened after the last_event_time
    """
    if not target_events:
        raise Exception("No target_events found/defined")

    if not target_acts:
        raise Exception("No target_acts found/defined")

    target_events = [f"'{event}'" for event in target_events]
    target_acts = [f"'{act}'" for act in target_acts]

    CUR.execute("""
    SELECT * FROM useinfo 
    WHERE useinfo.event IN ({events})
        AND useinfo.act IN ({acts})
        AND useinfo.timestamp > CAST('{last_event_time}' AS TIMESTAMP);""".format(events=', '.join(target_events), acts=', '.join(target_acts), last_event_time=last_event_time))

    events = CUR.fetchall()
    logger.info(f"Fetched {len(events)} events")
    return events


def send_caliper_event():
    cron_job = os.getenv('CRON_NAME', "runestone_caliper_job")
    last_event_sent_time = get_last_event_time(cron_job, 'success')
    event_types = ['page']
    act_types = ['view']
    batch = []
    batch_size = os.getenv("BATCH_SIZE", 5)
    cron_status = ""
    try:
        events = fetch_events(last_event_sent_time, event_types, act_types)
        if len(events) != 0:
            # Set last_event_sent_time to the lastest event time from all the fetched events
            event_times = [event[1] for event in events]
            last_event_sent_time = max(event_times)

        # Loop through events and send events to caliper
        for event in events:
            if event.get('event'):
                if event.get('event') == 'page' and event.get('act') == 'view':
                    caliper_event = get_caliper_event(event, "ViewEvent", "Viewed")
                if caliper_event:
                    batch.append(caliper_event)

            if len(batch) == batch_size:
                send_event_batch(batch)
                batch = []

        if len(batch) != 0:
            send_event_batch(batch)
        # Return last event time
        logger.info(f"The last event happened at {last_event_sent_time}")
        cron_status = "success"
    except:
        logger.exception("Cannot send event")
        cron_status = "failure"
    return last_event_sent_time, cron_status


def get_caliper_event(event, event_type, event_action):
    # It's possible to not have chapters if it's a top level. This would result in -3 being 'published'
    if not event.get('div_id'):
        return None
    # This will split paths like /srv/web2py/applications/runestone/books/thinkcspy/published/thinkcspy/GeneralIntro/Algorithms.html
    # See the unit test code in the tests directory for more examples
    nav_path = event.get('div_id').split('/')
    if len(nav_path) < 3:
        return None

    # Need to try to figure out the document/chapter/page from the div id
    # If there's no scheme, add the file: scheme
    if not nav_path[0]:
        nav_path[0] = 'file:'
    document_path = '/'.join(nav_path[:-2]) + '/'
    chapter_path = '/'.join(nav_path[:-1]) + '/'

    # On the index page there may not be a chapter, just the document. The only way I can seem to tell this is if the name is published
    if 'published' == nav_path[-3]:
        is_part_of = caliper.entities.Document(
            id = document_path,
            name = nav_path[-2],
        )
    # Otherwise the chapter is part of the document
    else:
        is_part_of = caliper.entities.Chapter(
            id = chapter_path,
            name = nav_path[-2],
            isPartOf = caliper.entities.Document(
                id = document_path,
                name = nav_path[-3],
            )
        )

    # Page is either part of chapter & document or just part of the document
    resource = caliper.entities.Page(
        id='/'.join(nav_path),
        # Page name is always the last item
        name= nav_path[-1],
    )

    resource.isPartOf = is_part_of

    actor = caliper.entities.Person(id="urn:actor_id:" + event.get('sid'))
    course_id = os.getenv("COURSE_ID")
    edapp_id = os.getenv("EDAPP_ID")
    if not course_id or not edapp_id:
        raise Exception("You need to define both EDAPP_ID and COURSE_ID before using this.")
    organization = caliper.entities.Organization(id="urn:course_offering_id:" + course_id)
    edApp = caliper.entities.SoftwareApplication(id="url:edapp_id:" + edapp_id)
    the_event = None

    event_time = event.get('timestamp')
    if event_type == "NavigationEvent":
        the_event = caliper.events.NavigationEvent(
            actor=actor,
            edApp=edApp,
            group=organization,
            object=resource,
            eventTime=event_time.strftime('%Y-%m-%dT%H:%M:%S') + event_time.strftime('.%f')[:4] + 'Z',
            action=event_action
        )
    elif event_type == "ViewEvent":
        the_event = caliper.events.ViewEvent(
            actor=actor,
            edApp=edApp,
            group=organization,
            object=resource,
            eventTime=event_time.strftime('%Y-%m-%dT%H:%M:%S') + event_time.strftime('.%f')[:4] + 'Z',
            action=event_action
        )
    return the_event


def send_event_batch(batch):
    # Multiple LRW support: https://github.com/tl-its-umich-edu/python-caliper-tester
    lrw_type = os.getenv('LRW_TYPE', "").lower()
    token = os.getenv('LRW_TOKEN', "")
    lrw_server = os.getenv('LRW_SERVER', "")

    if lrw_type == 'unizin':
        lrw_endpoint = lrw_server
    elif lrw_type == 'ltitool':
        lrw_endpoint = "{lrw_server}/caliper/event?key={token}".format(lrw_server=lrw_server, token=token)
    else:
        sys.exit("LRW Type {lrw_type} not supported".format(lrw_type=lrw_type))

    the_config = caliper.HttpOptions(
        host="{0}".format(lrw_endpoint),
        auth_scheme='Bearer',
        api_key=token,
        debug=True)

    the_sensor = caliper.build_simple_sensor(
        sensor_id=os.getenv("SENSOR_ID", "{0}/test_caliper".format(lrw_server)),
        config_options=the_config)

    logger.info("Sending {} events".format(len(batch)))
    the_sensor.send(batch)

    logger.info(the_sensor.status_code)
    if (the_sensor.status_code != 200):
        if (the_sensor.debug):
            logger.info(pformat(the_sensor.debug[0].content))
        raise Exception(f"Exception sending events code is {the_sensor.status_code}")

    logger.info("event batch completed successfully!")


def update_runtime_table(last_event_time, cron_status):
    # Insert now into the runtime table after sending event
    now = datetime.utcnow()
    event_time = now.strftime('%Y-%m-%d %H:%M:%S')
    cron_name = os.getenv('CRON_NAME', "runestone_caliper_job")
    CUR.execute("""
    INSERT INTO cron_run_info (cron_job, last_run_time, last_run_status, last_sent_event_time) 
    VALUES ('{cron_job}', '{last_run_time}', '{last_run_status}', '{last_sent_event_time}');
    """.format(
        cron_job=cron_name,
        last_run_time=event_time,
        last_run_status=cron_status,
        last_sent_event_time=last_event_time))
    CONN.commit()

if __name__ == "__main__":
    main()
