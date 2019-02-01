import caliper
import psycopg2
import requests, json, sys, os, logging
from datetime import datetime, date, time
import os
from dotenv import Dotenv
import logging

# Configuration is for OpenLRW, obtain bearer token
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

logger.info("Connect to database...")
conn = psycopg2.connect(
    dbname = "runestone",
    user = os.getenv("USER"),
    password = os.getenv("PASSWORD")
    )
cur = conn.cursor()

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
    except Exception, err:
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
    except Exception, err:
        logger.error(err)
    return last_runtime

def fetch_events(last_runtime):
    # Fetch all events since last runtime
    try:
        cur.execute("SELECT * FROM useinfo WHERE useinfo.timestamp >= CAST('{}' AS TIMESTAMP);".format(last_runtime))
    except:
        cur.execute("SELECT * FROM useinfo")

    events = cur.fetchall()
    logger.info("Fetched {} events".format(len(events)))
    return events

def send_caliper_event():
    
    cron_job = 'test_cron'
    last_runtime = get_last_runtime(cron_job)
    events = fetch_events(last_runtime)

    # Loop through events and send events to caliper
    for event in events:
        try:
            user_id = event[2]
        except: # if no user_id
            continue

        try:
            evnt = event[3]
        except:
            evnt = ""

        try:
            act = event[4]
        except:
            act = ""

        try:
            course = event[6]
        except:
            continue

        try:
            nav_path = event[5].split('/')
        except:
            continue

        try:
            event_time = event[1]
        except:
            continue

        try:
            document = nav_path[3]
            document_path = '/'.join(nav_path[:4]) + '/'
        except:
            document = ""
            document_path = ""

        try:
            chapter = nav_path[4]
            chapter_path = '/'.join(nav_path[:5]) + '/'
        except:
            chapter = ""
            chapter_path = ""

        try:
            page = nav_path[5]
        except:
            page = ""

        resource = caliper.entities.Page(
                        id = '/'.join(nav_path),
                        name = page,
                        isPartOf = caliper.entities.Chapter(
                            id = chapter_path,
                            name = chapter,
                            isPartOf = caliper.entities.Document(
                                id = document_path,
                                name = document,
                            )
                        )
                    )

        actor = caliper.entities.Person(id=user_id)
        organization = caliper.entities.Organization(id="test_org")
        edApp = caliper.entities.SoftwareApplication(id=course)

        caliper_sender(
                    actor, 
                    organization, 
                    edApp, 
                    resource,
                    event_time)

def caliper_sender(actor, organization, course, resource, time):
    dotenv = Dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    os.environ.update(dotenv)

    # Multiple LRW support: https://github.com/tl-its-umich-edu/python-caliper-tester
    lrw_type = os.getenv('LRW_TYPE',"").lower()
    token = os.getenv('TOKEN',"")
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

    the_event = caliper.events.NavigationEvent(
            actor = actor,
            edApp = course,
            group = organization,
            object = resource,
            eventTime = time.isoformat(),
            action = "NavigatedTo"
            )

    # Once built, you can use your sensor to describe one or more often used
    # entities; suppose for example, you'll be sending a number of events
    # that all have the same actor

    # the_sensor.send(the_event)
    logger.info(dir(the_event))
    logger.info(the_sensor.send(the_event))

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