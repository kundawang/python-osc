Client
========

The client allows you to connect and exchange messages with an OSC server.
Client classes are available for UDP and TCP protocols.
The base client class ``send`` method expects an :class:`OSCMessage` object, which is then sent out over TCP or UDP.
Additionally, a simple client class exists that constructs the :class:`OSCMessage` object for you.

See the examples folder for more use cases.

Examples
---------

.. code-block:: python

    from pythonosc.udp_client import SimpleUDPClient

    ip = "127.0.0.1"
    port = 1337

    client = SimpleUDPClient(ip, port)  # Create client

    client.send_message("/some/address", 123)   # Send float message
    client.send_message("/some/address", [1, 2., "hello"])  # Send message with int, float and string

    # Alternatively, use a context manager to automatically close the socket
    with SimpleUDPClient(ip, port) as client:
        client.send_message("/some/address", 123)


Sending bounded bundles
~~~~~~~~~~~~~~~~~~~~~~~~

A bundle containing hundreds of updates can exceed the path's UDP datagram
limit and be dropped.  ``send_fragmented_bundle`` splits one bundle's elements
over several legal OSC bundles, each no larger than the given maximum datagram
size, and sends them in order.  Individual messages and nested bundles are
never split; if sending is interrupted, the raised ``BundleSendError`` exposes
the number of datagrams already sent through ``sent_count``.

.. code-block:: python

    from pythonosc.osc_bundle_builder import OscBundleBuilder
    from pythonosc.osc_message_builder import OscMessageBuilder
    from pythonosc.udp_client import UDPClient

    builder = OscBundleBuilder(0)
    for i in range(500):
        builder.add_content(OscMessageBuilder(address=f"/update/{i}").build())
    big_bundle = builder.build()

    client = UDPClient(ip, port)
    num_sent = client.send_fragmented_bundle(big_bundle, max_datagram_size=1024)

The splitting logic itself lives in :mod:`pythonosc.bundle_splitter` and can be
used without a socket, for example to inspect the produced datagrams first.


.. code-block:: python

    from pythonosc.tcp_client import SimpleTCPClient

    ip = "127.0.0.1"
    port = 1337

    client = SimpleTCPClient(ip, port)  # Create client

    client.send_message("/some/address", 123)   # Send float message
    client.send_message("/some/address", [1, 2., "hello"])  # Send message with int, float and string

Client Module Documentation
---------------------------------

.. automodule:: pythonosc.udp_client
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.bundle_splitter
  :special-members:
  :members:
  :exclude-members: __weakref__

.. automodule:: pythonosc.tcp_client
  :special-members:
  :members:
  :exclude-members: __weakref__
