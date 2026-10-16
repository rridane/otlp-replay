import os

import pytest


def pytest_collection_modifyitems(items):
    if os.environ.get("RAY_ADDRESS"):
        return
    skip = pytest.mark.skip(reason="Ray: only inside a Ray job")
    for item in items:
        if "ray" in item.keywords:
            item.add_marker(skip)
