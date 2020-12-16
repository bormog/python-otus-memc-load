import unittest
from main import appsinstalled_pb2


class TestProto(unittest.TestCase):

    def test_proto_serialize(self):
        sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
        for line in sample.splitlines():
            dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
            apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
            lat, lon = float(lat), float(lon)
            ua = appsinstalled_pb2.UserApps()
            ua.lat = lat
            ua.lon = lon
            ua.apps.extend(apps)
            packed = ua.SerializeToString()
            unpacked = appsinstalled_pb2.UserApps()
            unpacked.ParseFromString(packed)
            self.assertEqual(ua, unpacked)
