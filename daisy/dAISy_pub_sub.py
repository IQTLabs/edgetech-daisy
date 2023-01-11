"""_summary_   
"""
import os
from time import sleep, time
import json
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
        self.serial = serial.Serial(self.serial_port)

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
        # publish the data as a JSON to the topic
        success = self.publish_to_topic(self.send_data_topic, json.dumps(data))

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
        connection alive and publishes the data to the MQTT broker and keep the
        main thread alive.
        """
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )

        while True:
            try:
                if self.serial.in_waiting:
                    # send the payload to MQTT
                    self._send_data(
                        {
                            "timestamp": str(time()),
                            "data": self.serial.readline().decode(),
                        }
                    )

                # flush any scheduled processes that are waiting
                schedule.run_pending()
                # prevent the loop from running at CPU time
                sleep(0.001)

            except KeyboardInterrupt:
                # if keyboard interrupt, fail gracefully
                self._disconnect_serial()
                if self.debug:
                    print("AIS application stopped!")

            except Exception as e:
                print(e)


if __name__ == "__main__":
    sender = DAISyPubSub(
        serial_port=os.environ.get("SERIAL_PORT"),
        send_data_topic=os.environ.get("SEND_DATA_TOPIC"),
        mqtt_ip=os.environ.get("MQTT_IP"),
    )
    sender.main()
