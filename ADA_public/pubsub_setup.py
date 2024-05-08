import json
import logging

import time

from google.cloud import pubsub_v1


# Code is based on the following examples from Google. Please check them for more information.
# https://github.com/googleapis/python-pubsub/blob/master/samples/snippets/publisher.py
# https://github.com/googleapis/python-pubsub/blob/master/samples/snippets/subscriber.py


def create_topic(project_id, topic_id):
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        topic = publisher.create_topic(request={"name": topic_path})
        logging.info("Created topic: {}".format(topic.name))
    except Exception as ex:
        logging.info(
            ex
        )  # instead, can check if there is a topic already, and only if not create a new one


def publish_message(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()

    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    data = message.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print("Future returned:", future.result())


def create_subscription(project_id, topic_id, subscription_id):
    try:
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        with subscriber:
            subscription = subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
        logging.info(f"Subscription created: {subscription}")
    except Exception as ex:
        logging.info(
            f"Error creating subscription {subscription_id} , the exception: {ex}."
        )
        logging.info(ex)


def pull_message(project, subscription):
    subscription_name = "projects/{project_id}/subscriptions/{sub}".format(
        project_id=project, sub=subscription
    )

    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(subscription_name, callback)
        try:
            future.result()
        except Exception as ex:
            logging.info(
                f"Listening for messages on {subscription_name} threw an exception: {ex}."
            )
            time.sleep(30)

    return future


def callback(message):
    logging.info(f"Received {message}.")

    return message
    message.ack()
