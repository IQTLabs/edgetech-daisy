import serial
import time
import json
import schedule
import os

from BaseMQTTPubSub import BaseMQTTPubSub
class aisDataSender(BaseMQTTPubSub):
    
    def __init__(self, serialport='/dev/serial0', dataChannel='/aisonobuoy/dAISy', mqttIP='', verbose=False):
        BaseMQTTPubSub.__init__(self)
        self.verbose = verbose
        self.SERIAL_PORT = serialport
        if mqttIP:
            self.client_connection_parameters['IP'] = mqttIP
        print(f'Connecting to MQTT bus on {self.client_connection_parameters["IP"]}')
        BaseMQTTPubSub.connect_client(self)
        self.publish_to_topic('/registration',f'dAISy Sender Registration')
        self.dataChannel = dataChannel
        self.connectSerial()
    def connectSerial(self):
        self.ser = serial.Serial(self.SERIAL_PORT)
        print(f'Connecting to Serial Bus on {self.SERIAL_PORT}')
    def disconnectSerial(self):
        self.ser.close()
    def heartbeat(self):
        self.publish_to_topic('/heartbeat',f'dAISy Sender Heartbeat')
    def sendData(self, data):
        self.publish_to_topic(self.dataChannel,json.dumps(data))
        if self.verbose:
            print(f'Sent data on channel {self.dataChannel}: {json.dumps(data)}')
    def main(self):
        running = True
        schedule.every(10).seconds.do(self.heartbeat)
        print('System Initialized and Running')
        while running:
            try:
                if self.ser.in_waiting:
                    data = {'timereceived': time.time(), 'data': self.ser.readline().decode()}
                    self.sendData(data)
                schedule.run_pending()
                time.sleep(0.001)
            except KeyboardInterrupt:
                running = False
                self.disconnectSerial(self)
                print("AIS application stopped!")
            except Exception as e:
                print(e)
            except:
                print('Unknown problem')


sender = aisDataSender(mqttIP=os.environ.get('mqttbus'))
sender.main()
