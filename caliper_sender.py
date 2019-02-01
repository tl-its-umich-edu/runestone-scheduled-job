import caliper
import psycopg2
import requests, json, sys, os, logging
from datetime import datetime, date, time
import os

print("Connect to database...")
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
            (cron_job varchar(64) NOT NULL UNIQUE,
            last_run_time timestamp NOT NULL, 
            last_run_status varchar(255) NOT NULL);""")
    except Exception, err:
        print(err)

create_runtime_table()

def send_caliper_event():
    
    cron_job = 'test_cron'
    
    # Get last run time from database
    try:
        cur.execute("SELECT last_run_time FROM cron_run_info WHERE cron_job = '{}' ".format(cron_job))
        last_run = cur.fetchone()
        last_runtime = last_run[0].strftime('%Y-%m-%d %H:%M:%S')
    except Exception, err:
        print(err)

    # Fetch all events since last runtime
    try:
        cur.execute("SELECT * FROM useinfo WHERE useinfo.timestamp >= CAST('{}' AS TIMESTAMP);".format(last_runtime))
        events = cur.fetchall()
        print("Fetched {} events".format(len(events))) 
    except Exception, err:   
        print(err)

    # Loop through events and send events to caliper
    for event in events:
        user_id = event[2]
        evnt = event[3]
        act = event[4]
        course = event[6]
        nav_path = event[5].split('/')

        document_path = '/'.join(nav_path[:4]) + '/'
        document = nav_path[3]
        chapter_path = '/'.join(nav_path[:5]) + '/'
        chapter = nav_path[4]
        event_time = event[1]
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
    # TODO: lrw_server should come from environment variable
    lrw_server = "http://lti.tools"
    # TODO: Endpoint should probably also come from environment variable
    lrw_endpoint = lrw_server + "/caliper/event?key=python-caliper"

    # TODO: token should come from enviornment variable
    token = "python-caliper"

    the_config = caliper.HttpOptions(
        host="{0}".format(lrw_endpoint),
        auth_scheme='Bearer',
        api_key=token)

    the_sensor = caliper.build_sensor_from_config(
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

    ret = the_sensor.describe(the_event.actor)
    the_sensor.send(the_event)
    print("event sent!")



def update_runtime_table(): 
    # Insert now into the runtime table after sending event
    now = datetime.utcnow()
    event_time = now.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
    INSERT INTO cron_run_info (cron_job, last_run_time, last_run_status) 
    VALUES ('{cron_job}', '{last_run_time}', '{last_run_status}')
    ON CONFLICT (cron_job) DO UPDATE SET 
    	last_run_time = EXCLUDED.last_run_time,
        last_run_status = EXCLUDED.last_run_status;
    """.format(
        cron_job = 'test_cron', 
        last_run_time = event_time, 
        last_run_status = 'test_status'))
    conn.commit()

send_caliper_event()
update_runtime_table()