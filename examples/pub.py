events = {
    "user_login": ["subscriber_A", "subscriber_B"],
    "order_created": ["subscriber_C"],
    "payment_failed": ["subscriber_A", "subscriber_C"],
    "event_x": [],
}


def execute():
    print("a")


def other_func():
    print("b")


subscribers = {"subscriber_A": execute, "subscriber_B": other_func}


def publish(event):
    if event in events:
        for sub_id in events[event]:
            callback = subscribers[sub_id]
            callback()
