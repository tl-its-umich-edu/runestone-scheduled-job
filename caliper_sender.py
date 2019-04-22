# -*- coding: future_fstrings -*-
import caliper
import psycopg2
import psycopg2.extras

import requests, json, sys, os, logging
from datetime import datetime, date, time
import os
from dotenv import load_dotenv
import logging

# Configuration is for OpenLRW, obtain bearer token
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, this_dir + "/..")

dotenv = load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logger.info("Connect to database...")
conn = psycopg2.connect(
    dbname = os.getenv("DB_NAME", "runestone"),
    user = os.getenv("DB_USER", "runestone"),
    password = os.getenv("DB_PASS", "runestone"),
    host = os.getenv("DB_HOST", "localhost"),
    port = os.getenv("DB_PORT", 5432),
    )

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def create_runtime_table():
    """
    Create cron_run_info table if not exists
    """
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cron_run_info 
            (id SERIAL PRIMARY KEY,
            cron_job varchar(64) NOT NULL,
            last_run_time timestamp NOT NULL, 
            last_run_status varchar(255) NOT NULL,
            last_sent_event_time timestamp NOT NULL)
            ;""")
    except Exception as err:
        print(err)

create_runtime_table()

def get_last_event_time(cron_job, cron_status):
    """
    Return the last successfully sent event timestamp from the previous job
    """
    try:
        cur.execute("""
        SELECT last_sent_event_time FROM cron_run_info 
        WHERE cron_job = '{cron_job}' AND last_run_status = '{cron_status}'
        ORDER BY last_sent_event_time DESC LIMIT 1 """.format(cron_job = cron_job, cron_status = cron_status))
        last_event = cur.fetchone()
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

    cur.execute("""
    SELECT * FROM useinfo 
    WHERE useinfo.event IN ({events})
        AND useinfo.act IN ({acts})
        AND useinfo.timestamp > CAST('{last_event_time}' AS TIMESTAMP);""".format(events = ', '.join(target_events), acts = ', '.join(target_acts), last_event_time = last_event_time))

    events = cur.fetchall()
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
    nav_path = document_path = chapter_path = ""
    rsc = {}
    if event.get('div_id'):
        nav_path = event.get('div_id').split('/')
        document_path = '/'.join(nav_path[:4]) + '/'
        chapter_path = '/'.join(nav_path[:5]) + '/'
        if len(nav_path) > 3:
            rsc['document'] = nav_path[3]
        if len(nav_path) > 4:
            rsc['chapter'] = nav_path[4]
        if len(nav_path) > 5:
            rsc['page'] = nav_path[5]

    resource = caliper.entities.Page(
                    id = '/'.join(nav_path),
                    name = rsc.get('page'),
                    isPartOf = caliper.entities.Chapter(
                        id = chapter_path,
                        name = rsc.get('chapter'),
                        isPartOf = caliper.entities.Document(
                            id = document_path,
                            name = rsc.get('document'),
                        )
                    )
                )

    actor = caliper.entities.Person(id=event.get('sid'))
    organization = caliper.entities.Organization(id=os.getenv("COURSE_ID", ""))
    edApp = caliper.entities.SoftwareApplication(id=os.getenv('EDAPP_ID',""))
    the_event = None

    if event_type == "NavigationEvent":
        the_event = caliper.events.NavigationEvent(
            actor = actor,
            edApp = edApp,
            group = organization,
            object = resource,
            eventTime = event.get('timestamp').isoformat(),
            action = event_action
            )
    elif event_type == "ViewEvent":
        the_event = caliper.events.ViewEvent(
            actor = actor,
            edApp = edApp,
            group = organization,
            object = resource,
            eventTime = event.get('timestamp').isoformat(),
            action = event_action
        )
    return the_event

def send_event_batch(batch):
    # Multiple LRW support: https://github.com/tl-its-umich-edu/python-caliper-tester
    lrw_type = os.getenv('LRW_TYPE',"").lower()
    token = os.getenv('LRW_TOKEN',"")
    lrw_server = os.getenv('LRW_SERVER', "")

    if lrw_type == 'unizin':
        lrw_endpoint = lrw_server
    elif lrw_type == 'ltitool':
        lrw_endpoint = "{lrw_server}/caliper/event?key={token}".format(lrw_server = lrw_server, token = token)
    else:
        sys.exit("LRW Type {lrw_type} not supported".format(lrw_type = lrw_type))
    
    the_config = caliper.HttpOptions(
        host="{0}".format(lrw_endpoint),
        auth_scheme='Bearer',
        api_key=token,
        debug=True)

    the_sensor = caliper.build_simple_sensor(
            sensor_id = os.getenv("SENSOR_ID", "{0}/test_caliper".format(lrw_server)),
            config_options = the_config )
    
    logger.info("Sending {} events".format(len(batch)))
    the_sensor.send(batch)

    # logger.info(the_sensor.send(batch))
    logger.info (the_sensor.status_code)
    logger.info (the_sensor.debug) 
    if (the_sensor.status_code != 200):
        raise Exception(f"Exception sending events code is {the_sensor.status_code}")
        
    logger.info("event batch completed successfully!")


def update_runtime_table(last_event_time, cron_status): 
    # Insert now into the runtime table after sending event
    now = datetime.utcnow()
    event_time = now.strftime('%Y-%m-%d %H:%M:%S')
    cron_name = os.getenv('CRON_NAME', "runestone_caliper_job")
    cur.execute("""
    INSERT INTO cron_run_info (cron_job, last_run_time, last_run_status, last_sent_event_time) 
    VALUES ('{cron_job}', '{last_run_time}', '{last_run_status}', '{last_sent_event_time}');
    """.format(
        cron_job = cron_name, 
        last_run_time = event_time,  
        last_run_status = cron_status,
        last_sent_event_time = last_event_time))
    conn.commit()

last_event_time, cron_status = send_caliper_event()
update_runtime_table(last_event_time, cron_status)