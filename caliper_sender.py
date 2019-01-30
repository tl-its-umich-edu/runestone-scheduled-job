import caliper
import psycopg2
# from dotenv import load_dotenv
import requests, json, sys, os, logging
from datetime import datetime, date, time

import os

# load_dotenv()

def send_caliper_event():
    print("Connect to database...")
    conn = psycopg2.connect(
        dbname = "runestone",
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD")
        )
    cur = conn.cursor()

    # TODO: create a runtime table, get the last runtime
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS execute_time (runtime timestamp NOT NULL)")
    # now = datetime.utcnow()
    # event_time = now.strftime('%Y-%m-%d %H:%M:%S') + now.strftime('.%f')[:4] + 'Z'
    # cur.execute("INSERT INTO runtime (runtime) VALUES (%s)" % (event_time))
    except Exception, err:
        print(err)
    
    d = date(2019, 1, 18)
    t = time(14, 30)
    test_time = datetime.combine(d, t)

    # get date from database after certain runtime
    try:
        cur.execute("SELECT * FROM useinfo WHERE useinfo.timestamp >= CAST('{}' AS TIMESTAMP);".format(test_time))
        events = cur.fetchall()
        print(events[0]) # tuple (43, datetime.datetime(2019, 1, 18, 14, 44, 17), 'chuyao', 'page', 'view', '/runestone/static/thinkcspy/GeneralIntro/toctree.html', 'thinkcspy')
    except Exception, err:   
        print(err)

    # TODO: loop through events and send events to caliper
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
        try:
            page = nav_path[5]
        except:
            page = "NULL"

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
                    test_time)

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

send_caliper_event()