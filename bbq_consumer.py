"""
    This program listens for work messages contiously in the smoker queue and sends an alert 
    when certain conditions are met.

    Condition: Smoker temperature decreases by 15 or more dregrees in a 15-minute window.

    Author: Brendi Kargel
    Date: September 29, 2023

"""

import pika
import sys
import time
from collections import deque

# Declare program constants (typically constants are named with ALL_CAPS)

HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)

# Declare variables
queue1 = "01-smoker"
smoker_deque = deque(maxlen=5)
smoker_alert_time = 15.0
alert = "Alert!! Smoker temperature has dropped by more than 15 degrees in 2.5 minutes!"

# Define the callback for smoker
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    smoker_temp =  body.decode().split(",")
    temperature = [0]
    # Do not change to float if temp is "temp not recorded"
    if smoker_temp[1] != "temp not recorded":

    # Changing the temperature string to a float in order to do calculation
        temperature[0] = float(smoker_temp[1])
    
    # Adding the temp data to the right side of the deque
        smoker_deque.append(temperature[0])

    # Calculating the temp difference and creating the alert
    if len(smoker_deque) == 5:
                temperature_difference = [smoker_deque[i]-smoker_deque[-1]
                                          for i in range(0, (len(smoker_deque)-1),1)]
                if any(value > 15 for value in temperature_difference):
                    alert_needed = True

                    if alert_needed:
                         print(alert)

    print(f" [x] Smoker temperature is {smoker_temp[1]} at {smoker_temp[0]}")

    # when done with task, tell the user
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    if len(smoker_deque) % 5 == 0:
            smoker_deque.clear()    

    # define a main function to run the program
def main(HOST: str, queue1: str):
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
        print.info()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        ch = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue1, durable=True)

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
        ch.basic_consume(queue1, smoker_callback, auto_ack=False)

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
    main(HOST, queue1)
