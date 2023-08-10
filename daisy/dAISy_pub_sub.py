"""This file contains the DAISyPubSub class which is a child class of
BaseMQTTPubSub.  The DAISyPubSub reads data from a specified serial
port and publishes binary and decoded payloads to the MQTT broker.
"""
import ast
from datetime import datetime
import json
import logging
import os
import sys
from time import sleep
import traceback
from typing import Any, Dict, Union

import coloredlogs
import paho.mqtt.client as mqtt
from pyais import decode
from pyais.exceptions import UnknownMessageException, MissingMultipartMessageException
from pyais.messages import (
    MessageType1,
    MessageType2,
    MessageType3,
    MessageType4,
    MessageType18,
)
import schedule
import serial

from base_mqtt_pub_sub import BaseMQTTPubSub

STYLES = {
    "critical": {"bold": True, "color": "red"},
    "debug": {"color": "green"},
    "error": {"color": "red"},
    "info": {"color": "white"},
    "notice": {"color": "magenta"},
    "spam": {"color": "green", "faint": True},
    "success": {"bold": True, "color": "green"},
    "verbose": {"color": "blue"},
    "warning": {"color": "yellow"},
}
coloredlogs.install(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    fmt="%(asctime)s.%(msecs)03d \033[0;90m%(levelname)-8s "
    ""
    "\033[0;36m%(filename)-18s%(lineno)3d\033[00m "
    "%(message)s",
    level_styles=STYLES,
)


class DAISyPubSub(BaseMQTTPubSub):
    """This class creates a connection to the MQTT broker and to the
    dAISy serial port to publish binary and decoded AIS bytestrings to
    an MQTT topic.

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the
            EdgeTech Core module
    """

    def __init__(
        self: Any,
        hostname: str,
        daisy_serial_port: str,
        config_json_topic: str,
        ais_bytestring_topic: str,
        ais_json_topic: str,
        continue_on_exception: bool = False,
        **kwargs: Any,
    ):
        """The DAISyPubSub constructor takes a serial port address and
        after instantiating a connection to the MQTT broker also
        connects to the serial port specified.

        Args:
            hostname (str): Name of host
            daisy_serial_port (str): a serial port to subscribe
                to. Specified via docker-compose.
            config_json_topic (str): MQTT topic for subscribing to config
                messages
            ais_bytestring_topic (str): MQTT topic on which to
                publish AIS bytestring data
            ais_json_topic (str): MQTT topic on which to publish
                AIS JSON data
            continue_on_exception (bool): Continue on unhandled
                exceptions if True, raise exception if False (the default)
        """
        super().__init__(**kwargs)
        self.hostname = hostname
        self.daisy_serial_port = daisy_serial_port
        self.config_json_topic = config_json_topic
        self.ais_bytestring_topic = ais_bytestring_topic
        self.ais_json_topic = ais_json_topic
        self.continue_on_exception = continue_on_exception

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)
        self.publish_registration("dAISy Sender Registration")

        # Setup the serial connection
        self._connect_serial()

        # Log configuration parameters
        self._log_config()

    def decode_payload(
        self, msg: Union[mqtt.MQTTMessage, str], data_payload_type: str
    ) -> Dict[Any, Any]:
        """
        Decode the payload carried by a message.

        Parameters
        ----------
        payload: mqtt.MQTTMessage
            The MQTT message
        data_payload_type: str
            The data payload type

        Returns
        -------
        data : Dict[Any, Any]
            The data payload of the message payload
        """
        if type(msg) == mqtt.MQTTMessage:
            payload = msg.payload.decode()
        else:
            payload = msg
        data_payload = json.loads(payload)[data_payload_type]
        return json.loads(data_payload)

    def _config_callback(
        self,
        _client: Union[mqtt.Client, None],
        _userdata: Union[Dict[Any, Any], None],
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]],
    ) -> None:
        """
        Process config message.

        Parameters
        ----------
        _client: Union[mqtt.Client, None]
            MQTT client
        _userdata: Union[Dict[Any, Any], None]
            Any required user data
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]]
            An MQTT message, or dictionary

        Returns
        -------
        None
        """
        # Assign data attributes allowed to change during operation,
        # ignoring config message data without a "daisy" key
        data = self.decode_payload(msg, "Configuration")
        if "daisy" not in data:
            return
        logging.info(f"Processing config message data: {data}")
        config = data["daisy"]
        self.hostname = config.get("hostname", self.hostname)
        self.daisy_serial_port = config.get("daisy_serial_port", self.daisy_serial_port)
        self.config_json_topic = config.get("config_json_topic", self.config_json_topic)
        self.ais_bytestring_topic = config.get(
            "ais_bytestring_topic", self.ais_bytestring_topic
        )
        self.ais_json_topic = config.get("ais_json_topic", self.ais_json_topic)
        self.continue_on_exception = config.get(
            "continue_on_exception", self.continue_on_exception
        )

        # Log configuration parameters
        self._log_config()

    def _log_config(self: Any) -> None:
        """Logs all paramters that can be set on construction."""
        config = {
            "hostname": self.hostname,
            "daisy_serial_port": self.daisy_serial_port,
            "config_json_topic": self.config_json_topic,
            "ais_bytestring_topic": self.ais_bytestring_topic,
            "ais_json_topic": self.ais_json_topic,
            "continue_on_exception": self.continue_on_exception,
        }
        logging.info(f"DAISyPubSub configuration:\n{json.dumps(config, indent=4)}")

    def _connect_serial(self: Any) -> None:
        """Sets up a serial connection using python's serial package
        to the port specified in the constructor.
        """
        # Setup serial connection without blocking
        # dAISy default baud is 38400
        self.serial = serial.Serial(self.daisy_serial_port, timeout=0, baudrate=38400)
        logging.info(f"Connected to Serial Bus on {self.daisy_serial_port}")

    def _disconnect_serial(self: Any) -> None:
        """Disconnects the serial connection using python's serial
        package.
        """
        self.serial.close()
        logging.info(f"Disconnected from Serial Bus on {self.daisy_serial_port}")

    def _send_data(self: Any, data: Dict[str, str]) -> bool:
        """Leverages edgetech-core functionality to publish a JSON
        payload to the MQTT broker on the topic specified in the class
        constructor.

        Args:
            data (Dict[str, str]): Dictionary payload that maps keys
                to payload.

        Returns:
            bool: Returns True if successful publish else False.
        """
        # TODO: Provide fields via environment or command line
        out_json = self.generate_payload_json(
            push_timestamp=int(datetime.utcnow().timestamp()),
            device_type="Collector",
            id_=self.hostname,
            deployment_id=f"AISonobuoy-Arlington-{self.hostname}",
            current_location="-90, -180",
            status="Debug",
            message_type="Event",
            model_version="null",
            firmware_version="v0.0.0",
            data_payload_type=data["type"],
            data_payload=data["payload"],
        )

        # Publish the data as JSON to the topic by type
        if data["type"] == "Binary AIS":
            send_data_topic = self.ais_bytestring_topic

        elif data["type"] == "Decoded AIS":
            send_data_topic = self.ais_json_topic

        success = self.publish_to_topic(send_data_topic, out_json)
        if success:
            logging.info(f"Successfully sent data on channel {send_data_topic}: {data}")
        else:
            logging.info(f"Failed to send data on channel {send_data_topic}: {data}")

        # Return True if successful else False
        return success

    def process_serial_payload(self, binary_payload: str) -> None:
        """Publish the binary payload to MQTT, then attempt to decode
        the binary payload and, if successful, publish the decoded
        payload to MQTT.

        Args:
            binary_payload (str): Payload portion of utf-8 decoded
                serial bytestring
        """
        # Send the binary payload to MQTT
        timestamp = datetime.utcnow().timestamp()
        self._send_data(
            {
                "type": "Binary AIS",
                "payload": binary_payload,
            }
        )

        # Decode the binary payload
        try:
            decoded_payload = decode(binary_payload)

            # Process the decoded payload by type
            processed_payload = {}
            processed_payload["timestamp"] = timestamp
            message_type = type(decoded_payload)
            if message_type in [MessageType1, MessageType2, MessageType3]:
                # Class A AIS Position Report (Messages 1, 2, and 3)
                # See:
                #     https://www.navcen.uscg.gov/ais-class-a-reports
                #     https://gpsd.gitlab.io/gpsd/AIVDM.html#_types_1_2_and_3_position_report_class_a
                processed_payload["mmsi"] = decoded_payload.mmsi
                processed_payload["latitude"] = (
                    decoded_payload.lat * 10000 / 60
                )  # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["longitude"] = (
                    decoded_payload.lon * 10000 / 60
                )  # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["altitude"] = 0
                processed_payload["horizontal_velocity"] = (
                    decoded_payload.speed * 1852.0 / 3600
                )  # [knots] * [1852.000 m/hr / knot] / [3600 s/hr]
                processed_payload["course"] = decoded_payload.course
                # [deg]
                processed_payload["vertical_velocity"] = 0
                # Optional values
                processed_payload["second"] = decoded_payload.second
                # of UTC
                processed_payload["status"] = decoded_payload.status
                processed_payload["turn"] = decoded_payload.turn
                processed_payload["accuracy"] = decoded_payload.accuracy
                processed_payload["heading"] = decoded_payload.heading
                processed_payload["maneuver"] = decoded_payload.maneuver

            elif message_type == MessageType4:
                # AIS Base Station Report (Message 4) and Coordinated Universal Time and Date Response (Message 11)
                # See:
                #     https://www.navcen.uscg.gov/ais-base-station-report-message4-coordinated-universal-time-date-mesponse-message11
                #     https://gpsd.gitlab.io/gpsd/AIVDM.html#_type_4_base_station_report
                processed_payload["mmsi"] = decoded_payload.mmsi
                processed_payload["latitude"] = decoded_payload.lat * 10000 / 60
                # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["longitude"] = decoded_payload.lon * 10000 / 60
                # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["altitude"] = 0
                processed_payload["horizontal_velocity"] = 0
                processed_payload["course"] = 0
                processed_payload["vertical_velocity"] = 0
                # Optional values
                processed_payload["year"] = decoded_payload.year
                # of UTC
                processed_payload["month"] = decoded_payload.month
                # of UTC
                processed_payload["day"] = decoded_payload.day
                # of UTC
                processed_payload["hour"] = decoded_payload.hour
                # of UTC
                processed_payload["minute"] = decoded_payload.minute
                # of UTC
                processed_payload["second"] = decoded_payload.second
                # of UTC
                processed_payload["accuracy"] = decoded_payload.accuracy

            elif message_type == MessageType18:
                # AIS Standard Class B Equipment Position Report (Message 18)
                # See:
                #     https://www.navcen.uscg.gov/ais-class-b-reports
                #     https://gpsd.gitlab.io/gpsd/AIVDM.html#_type_18_standard_class_b_cs_position_report
                processed_payload["mmsi"] = decoded_payload.mmsi
                processed_payload["latitude"] = (
                    decoded_payload.lat * 10000 / 60
                )  # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["longitude"] = (
                    decoded_payload.lon * 10000 / 60
                )  # [min / 10000] * 10000 / [60 min / deg]
                processed_payload["altitude"] = 0
                processed_payload["horizontal_velocity"] = (
                    decoded_payload.speed * 1852.0 / 3600
                )  # [knots] * [1852.000 m/hr / knot] / [3600 s/hr]
                processed_payload["course"] = decoded_payload.course
                processed_payload["vertical_velocity"] = 0
                # Optional values
                processed_payload["second"] = decoded_payload.second
                # of UTC
                processed_payload["accuracy"] = decoded_payload.accuracy
                processed_payload["heading"] = decoded_payload.heading

            else:
                logging.info(f"Skipping message type: {message_type}")
                return

            # Send the processed payload to MQTT
            self._send_data(
                {
                    "type": "Decoded AIS",
                    "payload": json.dumps(processed_payload),
                }
            )

        except UnknownMessageException as exception:
            logging.error(f"Could not decode binary payload: {exception}")

        ## TODO: Investigate why these are needed
        except MissingMultipartMessageException as exception:
            logging.error(f"Message Payload Composition error: {exception}")

    def main(self: Any) -> None:
        """Main loop to setup the heartbeat which keeps the TCP/IP
        connection alive, publish serial data to the MQTT broker, and
        keep the main thread alive.
        """
        # Schedule module heartbeat
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )

        # Subscribe to required topics
        self.add_subscribe_topic(self.config_json_topic, self._config_callback)

        logging.info("System initialized and running")
        payload_beginning = ""
        while True:
            try:
                # Read and handle waiting serial bytes
                if self.serial.in_waiting:
                    in_waiting = self.serial.in_waiting
                    logging.debug(f"Attempting to read {in_waiting} bytes in waiting")
                    serial_bytes = self.serial.read(in_waiting)
                    logging.debug(f"Read {in_waiting} bytes in waiting")

                    # Process required payloads when complete
                    serial_payloads = serial_bytes.decode().split("\n")
                    for serial_payload in serial_payloads:
                        logging.debug(f"Processing serial payload: {serial_payload}")
                        if "AIVDM" in serial_payload and "\r" in serial_payload:
                            # Payload is required and complete
                            logging.debug("Payload is required and complete")
                            self.process_serial_payload(serial_payload)

                        elif "AIVDM" not in serial_payload:
                            # Payload is not required, or not
                            # complete: first AIVDM payload ending
                            # only
                            logging.debug("Payload is not required, or not complete")
                            continue

                        elif "AIVDM" in serial_payload:
                            # Payload is required, but not complete: beginning only
                            logging.debug(
                                "Payload is required, but not complete: beginning only"
                            )
                            payload_beginng = serial_payload

                        elif payload_beginning != "":
                            # Payload is required, but not complete: ending only
                            logging.debug(
                                "Payload is required, but not complete: ending only"
                            )
                            logging.debug(
                                f"Complete payload: {payload_beginning + serial_payload}"
                            )
                            self.process_serial_payload(
                                payload_beginning + serial_payload
                            )
                            payload_beginning = ""

                # Flush any scheduled processes that are waiting
                schedule.run_pending()

                # Prevent the loop from running at CPU time
                sleep(0.001)

            except KeyboardInterrupt as exception:
                # If keyboard interrupt, fail gracefully
                logging.warning("Received keyboard interrupt: exiting gracefully")
                self._disconnect_serial()
                sys.exit()

            except Exception as exception:
                # Optionally continue on exception
                if self.continue_on_exception:
                    traceback.print_exc()
                else:
                    raise


def make_daisy() -> DAISyPubSub:
    """Instantiates DAISyPubSub."""
    return DAISyPubSub(
        mqtt_ip=os.environ.get("MQTT_IP", ""),
        hostname=os.environ.get("HOSTNAME", ""),
        daisy_serial_port=os.environ.get("DAISY_SERIAL_PORT", ""),
        config_json_topic=os.getenv("CONFIG_JSON_TOPIC", ""),
        ais_bytestring_topic=os.environ.get("AIS_BYTESTRING_TOPIC", ""),
        ais_json_topic=os.environ.get("AIS_JSON_TOPIC", ""),
        continue_on_exception=ast.literal_eval(
            os.environ.get("CONTINUE_ON_EXCEPTION", "False")
        ),
    )


if __name__ == "__main__":
    # Instantiate DAISyPubSub and execute
    diasy = make_daisy()
    diasy.main()
