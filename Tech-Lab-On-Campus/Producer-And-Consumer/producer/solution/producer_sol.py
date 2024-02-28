# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pika
import os
from producer_interface import *

class mqProducer(mqProducerInterface):
    def __init__(self, exchange_name: str) -> None:
        # Save parameters to class variables
        self.m_routing_key = routing_key
        self.m_exchange_name = exchange_name
        # Call setupRMQConnection
        self.setupRMQConnection()
        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.m_connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.m_channel = self.m_connection.channel()
        # Create the topic exchange if not already present
        self.m_channel.exchange_declare(self.m_exchange_name)
        pass

    def publishOrder(self, message: str) -> None:
        # Create Appropiate Topic String
        channel.exchange_declare(exchange="Exchange Name", exchange_type="topic")
        # Send serialized message or String
        properties = pika.BasicProperties(delivery_mode=2)
        self.m_channel.basic_publish(exchange=self.m_exchange_name,
                                routing_key=self.m)
        if __name__ == "__main __":
            print(" [x] Sent %r" % message)
        # Print Confirmation
        print('Published Message:', message)
        # Close channel and connection
        self.m_channel.close()
        self.m_connection.close()
        pass