from invoke.collection import Collection

from .check import check_ns
from .data import data_ns
from .test import test_ns

ns = Collection()
ns.configure({"run": {"pty": True}})
ns.add_collection(check_ns)
ns.add_collection(data_ns)
ns.add_collection(test_ns)
