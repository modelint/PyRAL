from collections import namedtuple
from pyral.rtypes import header, body, Attribute

# Test fixtures
Waypoint = namedtuple('Waypoint', ['WPT_number', 'Lat', 'Long', 'Frequency'])

def test_header_single_attribute():
    attrs = [Attribute(name='ID', type='int')]
    assert header(attrs) == "{ID int}"

def test_header_multiple_attributes():
    attrs = [
        Attribute(name='WPT number', type='int'),
        Attribute(name='Lat', type='string'),
        Attribute(name='Long', type='string'),
        Attribute(name='Frequency', type='double')
    ]
    expected = "{WPT_number int Lat string Long string Frequency double}"
    assert header(attrs) == expected

def test_body_single_tuple():
    t = Waypoint(WPT_number=3, Lat="37° 46' 30\" N", Long="-122° 25' 10\"", Frequency="117.95")
    expected = '{WPT_number {3} Lat {37° 46\' 30" N} Long {-122° 25\' 10"} Frequency {117.95}}'
    assert body([t]) == expected

def test_body_multiple_tuples():
    t1 = Waypoint(WPT_number=1, Lat="35° 12'", Long="-118° 22'", Frequency="113.20")
    t2 = Waypoint(WPT_number=2, Lat="36° 10'", Long="-119° 15'", Frequency="115.10")
    expected = (
        '{WPT_number {1} Lat {35° 12\'} Long {-118° 22\'} Frequency {113.20}} '
        '{WPT_number {2} Lat {36° 10\'} Long {-119° 15\'} Frequency {115.10}}'
    )
    assert body([t1, t2]) == expected

def test_body_empty():
    assert body([]) == ""
