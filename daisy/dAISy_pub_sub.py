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

import paho.mqtt.client as mqtt
import schedule
import serial

from base_mqtt_pub_sub import BaseMQTTPubSub


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
        ais_bytestring_topic: str,
        log_level: str = "INFO",
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
            ais_bytestring_topic (str): MQTT topic on which to
                publish AIS bytestring data
            log_level (str): One of 'NOTSET', 'DEBUG', 'INFO', 'WARN',
                'WARNING', 'ERROR', 'FATAL', 'CRITICAL'
            continue_on_exception (bool): Continue on unhandled
                exceptions if True, raise exception if False (the default)
        """
        super().__init__(**kwargs)
        self.hostname = hostname
        self.daisy_serial_port = daisy_serial_port
        self.ais_bytestring_topic = ais_bytestring_topic
        self.log_level = log_level
        self.continue_on_exception = continue_on_exception

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)
        self.publish_registration("dAISy Sender Registration")

        # Setup the serial connection
        self._connect_serial()

        # Log configuration parameters
        logging.info(
            f"""DAISyPubSub initialized with parameters:
    hostname = {hostname}
    daisy_serial_port = {daisy_serial_port}
    ais_bytestring_topic = {ais_bytestring_topic}
    log_level = {log_level}
    continue_on_exception = {continue_on_exception}
            """
        )

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
            push_timestamp=str(datetime.utcnow().timestamp()),
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

        # Publish the output JSON to the topic
        success = self.publish_to_topic(self.ais_bytestring_topic, out_json)
        if success:
            logging.info(
                f"Successfully sent data on channel {self.ais_bytestring_topic}: {data}"
            )
        else:
            logging.info(
                f"Failed to send data on channel {self.ais_bytestring_topic}: {data}"
            )
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

    def main(self: Any) -> None:
        """Main loop to setup the heartbeat which keeps the TCP/IP
        connection alive, publish serial data to the MQTT broker, and
        keep the main thread alive.
        """
        # Schedule module heartbeat
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )

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


if __name__ == "__main__":
    # Instantiate DAISyPubSub and execute
    daisy = DAISyPubSub(
        hostname=os.environ.get("HOSTNAME", ""),
        mqtt_ip=os.environ.get("MQTT_IP", ""),
        daisy_serial_port=os.environ.get("DAISY_SERIAL_PORT", ""),
        ais_bytestring_topic=os.environ.get("AIS_BYTESTRING_TOPIC", ""),
        log_level=os.environ.get("LOG_LEVEL"),
        continue_on_exception=ast.literal_eval(
            os.environ.get("CONTINUE_ON_EXCEPTION", "False")
        ),
    )
    daisy.main()
