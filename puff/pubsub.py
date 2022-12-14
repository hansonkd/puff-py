from typing import Optional, Any
from . import wrap_async, rust_objects


class PubSubMessage:
    from_connection_id: str
    body: bytes
    text: Optional[str]

    def json(self) -> Any:
        """
        Decode the message body and return a Python object.
        """
        pass


class PubSubConnection:
    def __init__(self, conn=None):
        self.conn = conn

    def who_am_i(self) -> str:
        """
        Return the current ID of this pubsub connection.
        """
        return self.conn.who_am_i()

    def receive(self) -> Optional[PubSubMessage]:
        """
        Block until a new message from one of the subscribed channels
        """
        return wrap_async(lambda rr: self.conn.receive(rr))

    def subscribe(self, channel: str) -> bool:
        """
        Subscribe to recevie new messages from channel.
        """
        return wrap_async(lambda rr: self.conn.subscribe(rr, channel))

    def unsubscribe(self, channel: str) -> bool:
        """
        Unscubscribe from new messages from channel.
        """
        return wrap_async(lambda rr: self.conn.unsubscribe(rr, channel))

    def publish(self, channel: str, message: str) -> bool:
        """
        Publish a message on the channel as a string.
        """
        return wrap_async(lambda rr: self.conn.publish(rr, channel, message))

    def publish_bytes(self, channel: str, message: bytes) -> bool:
        """
        Publish a message on the channel as raw bytes.
        """
        return wrap_async(lambda rr: self.conn.publish_bytes(rr, channel, message))

    def publish_json(self, channel: str, message: Any) -> bool:
        """
        Encode a Python object into JSON and send it to the channel.
        """
        return wrap_async(lambda rr: self.conn.publish_json(rr, channel, message))


class PubSubClient:
    def __init__(self, conn=None, client_fn=None):
        self.conn = conn
        self.client_fn = client_fn or rust_objects.global_pubsub_getter

    def client(self):
        ps = self.conn
        if ps is None:
            self.conn = ps = self.client_fn()
        return ps

    def new_connection_id(self) -> str:
        """
        Return a new ID of this pubsub connection.
        """
        return self.client().new_connection_id()

    def publish_as(self, connection_id: str, channel: str, message: str) -> bool:
        """
        Publish a message on the channel as a string as the ID.
        """
        return wrap_async(
            lambda rr: self.client().publish_as(rr, connection_id, channel, message),
        )

    def publish_bytes_as(
        self, connection_id: str, channel: str, message: bytes
    ) -> bool:
        """
        Publish a message on the channel as a bytes as the ID.
        """
        return wrap_async(
            lambda rr: self.client().publish_bytes_as(
                rr, connection_id, channel, message
            )
        )

    def publish_json_as(self, connection_id: str, channel: str, message: Any) -> bool:
        """
        Publish a message on the channel as json encoded string as the ID.
        """
        return wrap_async(
            lambda rr: self.client().publish_json_as(
                rr, connection_id, channel, message
            )
        )

    def connection(self) -> PubSubConnection:
        """
        Get a new PubSub connection.
        """
        return PubSubConnection(self.client().connection())

    def connection_with_id(self, connection_id: str) -> PubSubConnection:
        """
        Get a new PubSub connection that will send messages with specified ID.
        """
        return PubSubConnection(self.client().connection_with_id(connection_id))


global_pubsub = PubSubClient()


def named_client(name: str = "default"):
    return PubSubClient(
        client_fn=lambda: rust_objects.global_pubsub_getter.by_name(name)
    )
