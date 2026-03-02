from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.main import app


def test_index_route_registered() -> None:
    paths = {route.path for route in app.routes}
    assert "/" in paths
