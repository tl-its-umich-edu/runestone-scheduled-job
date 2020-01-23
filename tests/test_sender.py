import os, sys, unittest

import caliper

from dateutil.parser import parse

# Add this path first so it picks up the newest changes without having to rebuild
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, this_dir + "/..")
import caliper_sender as cs

class TestSender(unittest.TestCase):
    def test_get_caliper_event_chapter(self):
        event = {
            "div_id": "/srv/web2py/applications/runestone/books/thinkcspy/published/thinkcspy/GeneralIntro/Algorithms.html", 
            "sid": "test",
            "timestamp": parse("2020-01-16 17:24:50")
        }
        event = cs.get_caliper_event(event, "ViewEvent", "Viewed")
        self.assertTrue(isinstance(event, caliper.events.ViewEvent))
        self.assertTrue(isinstance(event.object, caliper.entities.Page))
        self.assertTrue(isinstance(event.object.isPartOf, caliper.entities.Chapter))
        self.assertTrue(isinstance(event.object.isPartOf.isPartOf, caliper.entities.Document))

    def test_get_caliper_event_document(self):
        event = {
            "div_id": "/opt/web2py/applications/runestone/books/fopp/published/fopp/index.html", 
            "sid": "test",
            "timestamp": parse("2020-01-16 17:24:50")
        }
        event = cs.get_caliper_event(event, "ViewEvent", "Viewed")
        self.assertTrue(isinstance(event, caliper.events.ViewEvent))
        self.assertTrue(isinstance(event.object, caliper.entities.Page))
        self.assertTrue(isinstance(event.object.isPartOf, caliper.entities.Document))
        self.assertTrue(event.object.isPartOf.isPartOf == None)

if __name__ == '__main__':
    unittest.main()