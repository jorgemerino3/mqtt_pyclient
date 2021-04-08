#!/usr/bin/python
#author: Dr. Jorge Merino
#organisation: University of Cambridge. Institute for Manufacturing.
#date: 2020.04.29
#license: GPL-3.0 (GNU General Public License version 3): https://opensource.org/licenses/GPL-3.0

import logging
import traceback
import paho.mqtt.client as mqtt

#=====================================================================#
#=========================== MQTT ====================================#
#=====================================================================#

class mqtt_client():
    def __init__(self, client_id, host=None, port=None):
        super().__init__()
        self.client_id=client_id
        self.host=host
        self.port=port
        self.connected=False
        self.disconnected=False
        self.topics_sub=list()
        self.client = mqtt.Client(client_id)
        #self.client = mqtt.Client(client_id=mqttcfg["user"], clean_session=True, userdata=None)
        self.client.on_connect = self.on_connect
        #self.client.on_disconnect = self.on_disconnect #only if loop is handled manually (i.e., loop_start() + loop_end() to reconnect)
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish

        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting. Fine for handling subscriptions only.
        #self.client.loop_forever()
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.

    def connect(self, mqttcfg=None, host=None, port=None):
        if(host!=None and port!=None):
            self.host = host
            self.port = port
        elif(isinstance(mqttcfg, dict) and mqttcfg["host"]!=None and mqttcfg["port"]!=None):
            self.host = mqttcfg["host"]
            self.port=mqttcfg["port"]
        #print(str(self.host) + ":" + str(self.port))
        if(self.host!=None and self.port!=None):
            try:
                logging.info("MQTT " + self.client_id + " - Connecting to " + self.host + ":" + str(self.port))
                self.client.connect(self.host, self.port)
                self.client.loop_start()
            except Exception as e:
                traceback.print_stack()
        else:
            logging.error("MQTT " + self.client_id + " - Connection configuration not provided")

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        #print(str(self.host) + ":" + str(self.port))
        if(rc==0):
            logging.info("MQTT " + self.client_id + " - Connected to MQTT Broker " + str(self.host) + ":" + str(self.port))
            self.connected=True
            self.disconnected=False
        elif(rc==1):
            logging.error("MQTT " + self.client_id + " - Connection refused: incorrect protocol version, check mqtt.conf")
        elif(rc==2):
            logging.error("MQTT " + self.client_id + " - Connection refused: invalid client identifier, check mqtt.conf")
        elif(rc==3):
            logging.error("MQTT " + self.client_id + " - Connection refused: server unavailable, check mqtt.conf")
        elif(rc==4):
            logging.error("MQTT " + self.client_id + " - Connection refused - bad username or password, check mqtt.conf")
        elif(rc==5):
            logging.error("MQTT " + self.client_id + " - Connection refused - not authorised by the broker, check mqtt.conf")
        else:
            logging.error("MQTT " + self.client_id + " - Connection refused - reason not provided, check with the broker administrator")

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        # self.client.subscribe("topic1", 0)
        # self.client.subscribe("topic2", 0)
        for topic in self.topics_sub:
            self.client.subscribe(topic)

    def disconnect(self):
        self.disconnected=True
        self.connected=False
        self.client.disconnect()
        self.client.loop_stop()

    def on_disconnect(self, client, userdata, rc):
        self.conected=False
        if (rc != 0):
            logging.warning("MQTT " + self.client_id + " - Unexpected disconnection")
        else:
            logging.warning("MQTT " + self.client_id + " - Controlled disconnection")
        if(not self.disconnected):
            self.connect()

    def subscribe(self, topic):
        if (not topic in self.topics_sub):
            self.topics_sub.append(topic)
            self.client.subscribe(topic)
            logging.info("MQTT " + self.client_id + " - Subscribed to topic " + topic)
        else:
            logging.warning("MQTT  " + self.client_id + " - Already subscribed to topic " + topic)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        message=str(msg.payload.decode("utf-8"))
        #print("message = " + message)
        topic=str(msg.topic)

        logging.info("MQTT " + self.client_id + " <<" + msg.topic + ">>: " + str(msg.payload.decode("utf-8")))
        #logging.info("message topic=" + msg.topic)
        #logging.info("message qos=" + msg.qos)
        #logging.info("message retain flag=",msg.retain)

    def publish(self, topic, data):
        
        if(self.connected):
            self.client.publish(topic, payload=str(data), qos=0, retain=False)

    # The callback for when the client published a message.
    def on_publish(self, client, flags, rc):
        logging.info("MQTT " + self.client_id + " - Message published to broker, connack code: "+str(rc))
