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
            last_run_status varchar(255) NOT NULL);""")
    except Exception as err:
        print(err)

create_runtime_table()

def get_last_runtime(cron_job):
    # Get last run time from database
    try:
        cur.execute("""
        SELECT last_run_time FROM cron_run_info 
        WHERE cron_job = '{}'
        ORDER BY last_run_time DESC LIMIT 1 """.format(cron_job))
        last_run = cur.fetchone()
        last_runtime = last_run[0].strftime('%Y-%m-%d %H:%M:%S')
    except Exception as err:
        logger.error(err)
        last_runtime = None
    return last_runtime

def fetch_events(last_runtime):
    # Fetch all events since last runtime
    if last_runtime:
        cur.execute("SELECT * FROM useinfo WHERE useinfo.timestamp >= CAST('{}' AS TIMESTAMP);".format(last_runtime))
    else:
        cur.execute("SELECT * FROM useinfo")

    events = cur.fetchall()
    logger.info("Fetched {} events".format(len(events)))
    return events

def send_caliper_event():
    
    cron_job = 'test_cron'
    last_runtime = get_last_runtime(cron_job)
    events = fetch_events(last_runtime)
    batch = []
    batch_size = os.getenv("BATCH_SIZE", 5)
    # print (events)
    # Loop through events and send events to caliper
    for event in events:
        nav_path = document_path = chapter_path = page = ""
        if event.get('div_id'):
            nav_path = event.get('div_id').split('/')
            document_path = '/'.join(nav_path[:4]) + '/'
            chapter_path = '/'.join(nav_path[:5]) + '/'
            if (len(nav_path) == 4):
                page = nav_path[5]

        resource = caliper.entities.Page(
                        id = '/'.join(nav_path),
                        name = page,
                        isPartOf = caliper.entities.Chapter(
                            id = chapter_path,
                            name = event.get('chapter'),
                            isPartOf = caliper.entities.Document(
                                id = document_path,
                                name = event.get('document'),
                            )
                        )
                    )

        actor = caliper.entities.Person(id=event.get('sid'))
        organization = caliper.entities.Organization(id="test_org")
        edApp = caliper.entities.SoftwareApplication(id=event.get('course_id'))

        the_event = caliper.events.NavigationEvent(
            actor = actor,
            edApp = edApp,
            group = organization,
            object = resource,
            eventTime = event.get('timestamp').isoformat(),
            action = "NavigatedTo"
            )
        
        batch.append(the_event)
        if len(batch) == batch_size:
            send_event_batch(batch)
            batch = []
            
    if len(batch) != 0:
        send_event_batch(batch)

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
            sensor_id = "{0}/test_caliper".format(lrw_server),
            config_options = the_config )
    
    logger.info("Sending {} events".format(len(batch)))
    the_sensor.send(batch)

    # logger.info(the_sensor.send(batch))
    logger.info (the_sensor.status_code)
    logger.info (the_sensor.debug) 
    logger.info("event sent!")


def update_runtime_table(): 
    # Insert now into the runtime table after sending event
    now = datetime.utcnow()
    event_time = now.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
    INSERT INTO cron_run_info (cron_job, last_run_time, last_run_status) 
    VALUES ('{cron_job}', '{last_run_time}', '{last_run_status}');
    """.format(
        cron_job = 'test_cron', 
        last_run_time = event_time,  
        last_run_status = 'test_status'))
    conn.commit()

send_caliper_event()
update_runtime_table()