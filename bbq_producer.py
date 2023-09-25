"""
    This program sends a message every 30 seconds to a queue on the RabbitMQ server. Data is sent to one,
    two, or three queues depending on the row content of a csv file (smoker-temps.csv).
    
    Author: Brendi Kargel
    Date: September 18, 2023

"""

import pika
import sys
import webbrowser
import csv
import time

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Declare program constants (typically constants are named with ALL_CAPS)

host = "localhost"
port = 9999
address_tuple = (host, port)
queue1 = "01-smoker"
queue2 = "02-food-A"
queue3 = "03-food-B"

# Only opens the Admin website if show_offer = True
show_offer = False

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    global show_offer
    if show_offer:
        webbrowser.open_new("http://localhost:15672/#/queues")

def send_message(host: str, queue1: str, queue2: str, queue3: str, input_file: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue1 (str): the name of the queue
        queue2 (str): the name of the queue
        queue3 (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue1, durable=True)
        ch.queue_declare(queue=queue2, durable=True)
        ch.queue_declare(queue=queue3, durable=True)

        with open(input_file, 'r') as input_file:
            reader = csv.reader(input_file)
            # skip the header row
            header = next(reader)
            logger.info("Skipping header row")
            # for each row in the file
            for row in reader:
                # get row variables
                time_stamp, smoker_temp, foodA_temp, foodB_temp = row
                
                # create messages to send to the queues
                message1 = time_stamp, smoker_temp
                message2 = time_stamp, foodA_temp
                message3 = time_stamp, foodB_temp
                
                # encode the messages
                message1_encode = "," .join(message1).encode()
                message2_encode = "," .join(message2).encode()
                message3_encode = "," .join(message3).encode()              
                
                # use the channel to publish message1 to the queue
                # every message passes through an exchange
                ch.basic_publish(exchange="", routing_key=queue1, body=message1_encode)
                # print message1 to the console for the user
                logger.info(f" [x] Sent {message1} to {queue1}")
                # use the channel to publish message2 to the queue
                ch.basic_publish(exchange="", routing_key=queue2, body=message2_encode)
                # print message2 to the console for the user
                logger.info(f" [x] Sent {message2} to {queue2}")
                # use the channel to publish message3 to the queue
                ch.basic_publish(exchange="", routing_key=queue3, body=message3_encode)
                # print message3 to the console for the user
                logger.info(f" [x] Sent {message3} to {queue3}")
                # Wait 30 seconds between each message
                time.sleep(30)

    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    
    # Send messages to the queues
    send_message(host,queue1,queue2,queue3,"smoker-temps.csv") 