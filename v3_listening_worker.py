"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Brendi Kargel
    Date: September 29, 2023

"""

import pika
import sys
import time
from collections import deque


#Declare constants
HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)

# Declare variables
queue2 = "02-food-A"
food_deque = deque(maxlen=20)
food_alert_change = 1.0

def alert_triggered(temp_diff):
    print(f"Alert! The temperature of FoodA has dropped at least 1 degree in the past 10 minutes!")

def foodA_callback(ch, method, properties, body):
    try:
        # Decode the message from bytes to a string
        body_str = body.decode('utf-8')
        temperature = float(body_str.split(':')[1])

        # Add the temperature reading to the deque
        food_deque.append(temperature)

        # Check if the deque has at least 20 readings
        if len(food_deque) >= 20:
            # Initialize a flag to track if the temperature change threshold is met
            alert_needed = False

        # Calculate the temperature change over the last 20 readings
        temp_diff = [food_deque[i] - food_deque[i - 1] for i in range(-1, -20, -1)]

        # Check if any of the temperature changes exceed the threshold
        if any(temp_change > food_alert_change for temp_change in temp_diff):
                threshold_met = True

        # If the threshold is met, trigger the alert
        if alert_needed:
            alert_triggered

        # Clear the deque every 20 readings
        if len(food_deque) % 20 == 0:
            food_deque.clear()

    except ValueError:
        print("Invalid temperature value in message body.")
    except Exception as e:
        print(f"Error processing message: {str(e)}")


# define a main function to run the program
def main():
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={HOST}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        ch = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue2, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        ch.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        ch.basic_consume( queue=queue2, on_message_callback=foodA_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        ch.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main()
