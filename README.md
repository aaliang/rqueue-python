client bindings for [rqueue](https://github.com/aaliang/rqueue) in python

####Example usage
```.py
from rqueue import RQueueClient

#example usage
if __name__ == "__main__":
    import thread
    import time

    def receiver(_client):
        while True:
            (topic, message) = _client.next_notify()
            print "%s: %s" % (topic, message)

    try:
        client = RQueueClient("0.0.0.0", 6567)
        client.subscribe("hello")

        thread.start_new_thread(receiver, (client,))

        time.sleep(1)

        client.notify("hello", "world")

    except Exception as e:
        print e

    k = input('')
```
