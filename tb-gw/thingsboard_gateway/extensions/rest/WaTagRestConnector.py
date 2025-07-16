import threading
import time
import requests
import logging
import json

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

log = logging.getLogger("connector.rest")

class WaTagRestConnector(Connector, threading.Thread):
    def __init__(self, gateway, config, connector_type=None):
        super().__init__()
        self.daemon = True
        self._gateway = gateway
        self._config = config
        self._connector_type = connector_type or "rest"
        self._stopped = False
        self._connected = False

        self.name = config.get("name", "WaTagRestConnector")
        self._id = config.get("id", "default_id")
        self.url = config.get("url")
        self.http_method = config.get("httpMethod", "POST").upper()
        self.headers = config.get("headers", {})
        self.body = config.get("body", {})
        self.polling_interval = config.get("pollingInterval", 60000) / 1000  # milliseconds → seconds

        # 컨버터 로드
        converter_info = config.get("converter", {})
        try:
            module = TBModuleLoader.import_module("rest", converter_info.get("extension", "WaTagRestConverter"))
            converter_class = getattr(module, converter_info.get("class", "WaTagRestConverter"))
            self.converter = converter_class(config)
        except Exception as e:
            log.error(f"[WaTagRestConnector] Error loading converter: {e}")
            self.converter = None

        log.info(f"[WaTagRestConnector] Initialized with polling interval {self.polling_interval} sec")

    def open(self):
        log.info("[WaTagRestConnector] Connector thread started")
        self._stopped = False
        self._connected = True
        self.start()

    def run(self):
        while not self._stopped:
            try:
                response = None
                if self.http_method == "POST":
                    response = requests.post(self.url, headers=self.headers, json=self.body, timeout=10)
                else:
                    response = requests.get(self.url, headers=self.headers, timeout=10)

                if response and response.status_code == 200:
                    json_data = response.json()
                    if self.converter:
                        converted_data = self.converter.convert(self._config, json_data)
                        for device in converted_data:
                            self._gateway.send_to_storage(self.name, self._id, device)
                else:
                    log.warning(f"[WaTagRestConnector] HTTP error {response.status_code if response else 'no response'}")

            except Exception as e:
                log.exception(f"[WaTagRestConnector] Error during polling: {e}")

            time.sleep(self.polling_interval)

    def close(self):
        self._stopped = True
        self._connected = False
        log.info("[WaTagRestConnector] Connector stopped")

    def get_id(self):
        return self._id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self._config

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self._stopped

    def on_attributes_update(self, content):
        # WaTag REST connector doesn't handle attribute updates
        log.debug("[WaTagRestConnector] Received attribute update: %s", content)

    def server_side_rpc_handler(self, content):
        # WaTag REST connector doesn't handle RPC requests
        log.debug("[WaTagRestConnector] Received RPC request: %s", content)
        return {"error": "RPC not supported"}
