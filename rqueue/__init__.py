import socket


class RQueueClient:

    SUBSCRIBE = 1
    NOTIFY = 7

    PREAMBLE_SZ = 3

    def __init__(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        self.sock = sock

    def subscribe(self, topic):
        """
        Sends a subscribe to the server
        :param topic: the topic, convertible to a bytearray
        """
        topic_bytes = bytearray(topic)
        message = bytearray(ByteUtil.integer_to_byte_2(len(topic_bytes)))
        message.append(RQueueClient.SUBSCRIBE)
        self.sock.send(message + topic_bytes)

    def notify(self, topic, content):
        """
        Sends a notify to the server
        :param topic: the topic, convertible to a bytearray
        :param content: the content
        """
        topic_bytes = bytearray(topic)
        content_bytes = bytearray(content)
        topic_bytes_len = len(topic_bytes)
        message = bytearray(ByteUtil.integer_to_byte_2(topic_bytes_len + len(content_bytes) + 1))
        message.append(RQueueClient.NOTIFY)
        message.append(topic_bytes_len)
        self.sock.send(message + topic + content)

    def next_message(self):
        """
        Consumes the bytes on the wire that comprise one message, blocking until it is available
        :returns the payload portion of the message
        :rtype bytearray
        """
        preamble = bytearray(b" " * RQueueClient.PREAMBLE_SZ)
        p_view = memoryview(preamble)
        preamble_read = 0
        while True:
            just_read = self.sock.recv_into(p_view[preamble_read:], RQueueClient.PREAMBLE_SZ - preamble_read)
            preamble_read += just_read
            if preamble_read == RQueueClient.PREAMBLE_SZ:
                break

        payload_size = ByteUtil.byte_2_to_integer(preamble[:2])
        payload = bytearray(b" " * payload_size)
        payload_view = memoryview(payload)
        payload_read = 0

        while True:
            just_read = self.sock.recv_into(payload_view[payload_read:], payload_size - payload_read)
            payload_read += just_read
            if payload_read == payload_size:
                break

        return payload

    def next_notify(self):
        """
        consumes the next messages until a notify is parsed
        only notifies can be sent to the client anyways
        :returns a tuple of (topic, content)
        :rtype (bytearray, bytearray)
        """
        message = self.next_message()
        topic_size = message[0]
        topic = message[1:1+topic_size]
        content = message[1+topic_size:]

        return topic, content

class ByteUtil:
    @staticmethod
    def integer_to_byte_2(num):
        return [
            num >> 8 & 0xFF,
            num & 0xFF
        ]

    @staticmethod
    def byte_2_to_integer(arr):
        return arr[0] << 8 | arr[1]
