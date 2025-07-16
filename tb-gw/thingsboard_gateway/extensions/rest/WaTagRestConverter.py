from thingsboard_gateway.connectors.converter import Converter
import json
import logging
import time

log = logging.getLogger("rest")


class WaTagRestConverter(Converter):
    def __init__(self, config):
        self.__config = config

    def get_current_ts(self):
        return int(time.time() * 1000)

    def convert(self, config, data):
        try:
            json_data = json.loads(data) if isinstance(data, str) else data
            values = json_data.get("Values", [])
            result = []

            for tag in values:
                device_name = tag.get("Name", "UnknownTag")
                ts_data = {
                    "ts": self.get_current_ts(),
                    "values": {
                        "value": float(tag.get("Value", 0)),
                        "quality": int(tag.get("Quality", -1))
                    }
                }
                result.append({
                    "deviceName": device_name,
                    "deviceType": "default",
                    "telemetry": [ts_data],
                    "attributes": []
                })
            return result
        except Exception as e:
            log.error("Failed to convert WaTagRest data: %s", e)
            return []
