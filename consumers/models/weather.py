"""Contains functionality related to Weather"""
import logging
from confluent_kafka.cimpl import Message


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message: Message):
        """Handles incoming weather data"""
        value = message.value()
        self.temperature = value['temperature']
        self.status = value['status']
