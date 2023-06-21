"""This file contains the DAISyPubSub class which is a child class of
BaseMQTTPubSub.  The DAISyPubSub reads data from a specified serial
port and publishes binary and decoded payloads to the MQTT broker.
"""
import coloredlogs
from datetime import datetime
import logging
import os
import sys
from time import sleep
from typing import Any, Dict

from pyais import decode
from pyais.exceptions import UnknownMessageException
import serial
import schedule

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
    level=logging.INFO,
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
        serial_port: str,
        send_data_topic: str,
        hostname: str,
        debug: bool = False,
        **kwargs: Any,
    ):
        """The DAISyPubSub constructor takes a serial port address and
        after instantiating a connection to the MQTT broker also
        connects to the serial port specified.

        Args:
            serial_port (str): a serial port to subscribe
                to. Specified via docker-compose.
            send_data_topic (str): MQTT topic to publish the data from
                the port to. Specified via docker-compose.
            TODO: Remove?
            debug (bool, optional): If the debug mode is turned on,
                log statements print to stdout. Defaults to False.
        """
        super().__init__(**kwargs)
        # Convert contructor parameters to class variables
        self.serial_port = serial_port
        self.send_data_topic = send_data_topic
        self.hostname = hostname
        self.debug = debug

        # Connect to the MQTT client
        self.connect_client()
        sleep(1)

        # Publish a message after successful connection to the MQTT broker
        self.publish_registration("dAISy Sender Registration")

        # Setup the serial connection
        self._connect_serial()

    def _connect_serial(self: Any) -> None:
        """Sets up a serial connection using python's serial package
        to the port specified in the constructor.
        """
        # Setup serial connection
        self.serial = serial.Serial(self.serial_port, timeout=0.001)
        logging.debug(f"Connected to Serial Bus on {self.serial_port}")

    def _disconnect_serial(self: Any) -> None:
        """Disconnects the serial connection using python's serial
        package.
        """
        self.serial.close()
        logging.debug(f"Disconnected from Serial Bus on {self.serial_port}")

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
            push_timestamp=str(int(datetime.utcnow().timestamp())),
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

        # Publish the data as JSON to the topic
        success = self.publish_to_topic(self.send_data_topic, out_json)
        if success:
            logging.info(
                f"Successfully sent data on channel {self.send_data_topic}: {data}"
            )
        else:
            logging.info(
                f"Failed to send data on channel {self.send_data_topic}: {data}"
            )

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
        self._send_data(
            {
                "type": "Binary AIS",
                "payload": binary_payload,
            }
        )

        # Decode the binary payload
        try:
            decoded_payload = decode(binary_payload)

            # Send the decoded payload to MQTT
            self._send_data(
                {
                    "type": "Decoded AIS",
                    "payload": str(decoded_payload),
                }
            )

        except UnknownMessageException as exception:
            logging.error(f"Could not decode binary payload: {exception}")

    def main(self: Any) -> None:
        """Main loop to setup the heartbeat which keeps the TCP/IP
        connection alive, publish serial data to the MQTT broker, and
        keep the main thread alive.
        """
        # Schedule the heartbeat
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )

        payload_beginning = ""
        while True:
            try:
                # Read and handle waiting serial bytes
                if self.serial.in_waiting:
                    try:
                        serial_bytes = self.serial.read(self.serial.in_waiting)
                    except Exception as exception:
                        logging.warning(
                            f"Could not read serial bytes in waiting: {exception}"
                        )
                        continue

                    # Process required payloads when complete
                    serial_payloads = serial_bytes.decode().split("\n")
                    for serial_payload in serial_payloads:
                        if "AIVDM" in serial_payload and "\r" in serial_payload:
                            # Payload is required and complete
                            self.process_serial_payload(serial_payload)

                        elif "sync" in serial_payload:
                            # Payload is not required
                            continue

                        elif "AIVDM" in serial_payload:
                            # Payload is required, but not complete: beginning only
                            payload_beginng = serial_payload

                        elif payload_beginning != "":
                            # Payload is required, but not complete: ending only
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
                self._disconnect_serial()
                logging.error(exception)
                sys.exit()


if __name__ == "__main__":
    sender = DAISyPubSub(
        serial_port=str(os.environ.get("AIS_SERIAL_PORT")),
        send_data_topic=str(os.environ.get("AIS_SEND_DATA_TOPIC")),
        hostname=str(os.environ.get("HOSTNAME")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    sender.main()
