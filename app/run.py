import os
import sys
import json
import time
import logging
from dotenv import load_dotenv
from app.repositories import stock_data_repo
from app.util.stock_constant import stock_tickers
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

load_dotenv()

# AWS IoT Core endpoint
IOT_ENDPOINT = os.getenv("AWS_IOT_MQTT_ENDPOINT")  

# Path to your certificate file, private key & root CA file in environment variables
CERT_PATH = os.getenv("AWS_IOT_THING_CERTIFICATE_PATH")
PRIVATE_KEY_PATH = os.getenv("AWS_IOT_THING_PRIVATE_KEY_PATH")
ROOT_CA_PATH = os.getenv("AWS_IOT_THING_CA_CERTIFICATE_PATH")

# my Iot Thing name
CLIENT_ID = os.getenv("AWS_IOT_THING_NAME")  

# temporary topic name to publish data on mqtt test client
TOPIC = os.getenv("AWS_MQTT_SUBSCRIPTION_TOPIC")  


def main():
    
    # Initialize AWS IoT MQTT Client
    mqtt_client = AWSIoTMQTTClient(CLIENT_ID)

    # Configure with certificate-based authentication
    mqtt_client.configureEndpoint(IOT_ENDPOINT, 8883)
    mqtt_client.configureCredentials(ROOT_CA_PATH, PRIVATE_KEY_PATH, CERT_PATH)

    # Configure client settings
    mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
    mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite publish queueing
    mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
    mqtt_client.configureConnectDisconnectTimeout(10)  # 10 seconds
    mqtt_client.configureMQTTOperationTimeout(5)  # 5 seconds

    # Connect to AWS IoT Core
    logger.info("Connecting to AWS IoT Core...")
    try:
        mqtt_client.connect()
        logger.info("Connected to AWS IoT Core")
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        return
    
    # Main loop to publish stock data
    try:
        while True:
            logger.info(f"Fetching stock data at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            stock_data = get_stock_data(stock_tickers)

            if stock_data:
                payload = json.dumps({"stocks": stock_data})

                logger.info(
                    f"Publishing {len(stock_data)} stock records to IoT Core..."
                )
                mqtt_client.publish(TOPIC, payload, 1)
                logger.info("Published successfully")

            logger.info(f"Waiting 100 seconds before next fetch...")
            time.sleep(100)
    except KeyboardInterrupt:
        logger.info("Program stopped by user")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        mqtt_client.disconnect()
        logger.info("Disconnected from AWS IoT Core")


if __name__ == "__main__":
    main()