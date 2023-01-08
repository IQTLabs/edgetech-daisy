import os
import time
import json
import serial
import schedule

from base_mqtt_pub_sub import BaseMQTTPubSub


class DAISyPubSub(BaseMQTTPubSub):
    SERIAL_PORT = "/dev/serial0"
    SEND_DATA_TOPIC = "/aisonobuoy/dAISy"

    def __init__(
        self,
        serial_port: str = SERIAL_PORT,
        send_data_topic: str = SEND_DATA_TOPIC,
        debug: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.serial_port = serial_port
        self.send_data_topic = send_data_topic
        self.debug = debug

        self.connect_client()
        self.publish_registration("dAISy Sender Registration")

        self.connect_serial()

    def connect_serial(self):
        self.serial = serial.Serial(self.serial_port)

        if self.debug:
            print(f"Connected to Serial Bus on {self.serial_port}")

    def disconnect_serial(self):
        self.serial.close()

    def send_data(self, data):
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

    def main(self):
        running = True
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="dAISy Sender Heartbeat"
        )

        while running:
            try:
                if self.serial.in_waiting:

                    self.send_data(
                        {
                            "timereceived": time.time(),
                            "data": self.serial.readline().decode(),
                        }
                    )

                schedule.run_pending()
                time.sleep(0.001)

            except KeyboardInterrupt:
                running = False
                self.disconnect_serial()
                if self.debug:
                    print("AIS application stopped!")

            except Exception as e:
                print(e)


if __name__ == "__main__":
    sender = DAISyPubSub(mqtt_ip=os.environ.get("MQTT_IP"))
    sender.main()
