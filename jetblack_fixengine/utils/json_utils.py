"""JSON utilities"""

from datetime import datetime
from decimal import Decimal
import json
from json import JSONEncoder
from typing import Any, Mapping


class JSONFIXEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(self, Decimal):
            return float(o)
        elif isinstance(o, bytes):
            return o.decode('ascii')
        else:
            return super().default(o)


def dict_to_json(dct: Mapping[str, Any]) -> str:
    return json.dumps(dct, cls=JSONFIXEncoder)
