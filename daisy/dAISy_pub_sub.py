"""This file contains the DAISyPubSub class which is a child class of BaseMQTTPubSub. 
The DAISyPubSub reads data from a specified socket and publishes it to the MQTT broker.
"""
import os
from time import sleep
import json
from datetime import datetime
from typing import Any, Dict
import serial
import schedule

from base_mqtt_pub_sub import BaseMQTTPubSub


class DAISyPubSub(BaseMQTTPubSub):
    """This class creates a connection to the MQTT broker and to the dAISy serial port
    to publish AIS bytestrings to an MQTT topic

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the EdgeTech Core module
    """

    def __init__(
        self: Any,
        serial_port: str,
        send_data_topic: str,
        hostname: str,
        debug: bool = False,
        **kwargs: Any,
    ):
        """The DAISyPubSub constructor takes a serial port address and after
        instantiating a connection to the MQTT broker also connects to the serial
        port specified.

        Args:
            serial_port (str): a serial port to subscribe to. Specified via docker-compose.
            send_data_topic (str): MQTT topic to publish the data from the port to.
            Specified via docker-compose.
            debug (bool, optional): If the debug mode is turned on, log statements print to stdout.
            Defaults to False.
        """
        super().__init__(**kwargs)
        # convert contructor parameters to class variables
        self.serial_port = serial_port
        self.send_data_topic = send_data_topic
        self.hostname = hostname
        self.debug = debug

        # connect to the MQTT client
        self.connect_client()
        sleep(1)
        # publish a message after successful connection to the MQTT broker
        self.publish_registration("dAISy Sender Registration")

        # setup the serial connection
        self._connect_serial()

    def _connect_serial(self: Any) -> None:
        """Sets up a serial connection using python's serial package to the port specified
        in the constructor.
        """
        # setup serial connection
        self.serial = serial.Serial(self.serial_port, timeout=0.001)

        if self.debug:
            print(f"Connected to Serial Bus on {self.serial_port}")

    def _disconnect_serial(self: Any) -> None:
        """Disconnects the serial connection using python's serial package."""
        self.serial.close()

    def _send_data(self: Any, data: Dict[str, str]) -> bool:
        """Leverages edgetech-core functionality to publish a JSON payload to the MQTT
        broker on the topic specified in the class constructor.

        Args:
            data (Dict[str, str]): Dictionary payload that maps keys to payload.

        Returns:
            bool: Returns True if successful publish else False.
        """
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
            data_payload_type="AIS",
            data_payload=json.dumps(data),
        )

        # publish the data as a JSON to the topic
        success = self.publish_to_topic(self.send_data_topic, out_json)

        if self.debug:
            if success:
                print(
                    f"Successfully sent data on channel {self.send_data_topic}: {json.dumps(data)}"
                )
            else:
                print(
                    f"Failed to send data on channel {self.send_data_topic}: {json.dumps(data)}"
                )
        # return True if successful else False
        return success

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker and keeps the
        main thread alive.
        """
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )
        queue = []

        while True:
            try:
                if self.serial.in_waiting:
                    # send the payload to MQTT
                    serial_payload = self.serial.read(10).decode()
                    if "\n" not in serial_payload:
                        queue.append(serial_payload)
                    else:
                        split_payload = serial_payload.split("\n")
                        queue.append(split_payload[0])
                        self._send_data(
                            {
                                "timestamp": str(datetime.utcnow().timestamp()),
                                "data": "".join(queue),
                            }
                        )
                        queue = []
                        if len(split_payload) > 1:
                            queue.append(split_payload[-1])

                # flush any scheduled processes that are waiting
                schedule.run_pending()
                # prevent the loop from running at CPU time
                sleep(0.001)

            except KeyboardInterrupt as exception:
                # if keyboard interrupt, fail gracefully
                self._disconnect_serial()
                if self.debug:
                    print(exception)


if __name__ == "__main__":
    sender = DAISyPubSub(
        serial_port=str(os.environ.get("AIS_SERIAL_PORT")),
        send_data_topic=str(os.environ.get("AIS_SEND_DATA_TOPIC")),
        hostname=str(os.environ.get("HOSTNAME")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    sender.main()
