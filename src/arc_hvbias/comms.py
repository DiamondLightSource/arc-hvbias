# import logging
import socket
import warnings
from typing import Optional

import cothread

# Constants
CR = "\r"
TIMEOUT = 1.0  # Seconds
RECV_BUFFER = 4096  # Bytes


class Comms:
    def __init__(self, ip: str, port: int):
        # self._log = logging.getLogger(self.__class__.__name__)
        # logging.basicConfig(level=logging.DEBUG)

        self._endpoint = (ip, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(TIMEOUT)
        self._lock = cothread.RLock()

    def connect(self):
        print(f"Connecting to {self._endpoint[0]}:{self._endpoint[1]}")
        self._socket.connect(self._endpoint)
        # Clear initial connection messages
        # TODO: Are these useful? Use as confirmation of connection?
        # self._clear_socket()
        self.clear_socket()

    def disconnect(self):
        self._socket.close()

    def clear_socket(self):
        """Read from socket until we timeout"""
        while True:
            try:
                self._socket.recv(RECV_BUFFER)
            except socket.timeout:
                break

    @staticmethod
    def _format_message(message: bytes) -> bytes:
        """Format message for printing by appending a newline char.

        Args:
            message (bytes): The message to format.

        Returns:
            bytes: The formatted message.
        """
        return message + b"\n"

    def _send(self, request: bytes):
        """Send a request.

        Args:
            request (str): The request string to send.
        """
        # print(f"Sending request:\n{self._format_message(request)}")
        self._socket.send(self._format_message(request))
        # print(f"Sent {bytes_sent} byte(s)")

    def _send_receive(self, request: bytes) -> Optional[bytes]:
        """Sends a request and attempts to decode the response. Does not determine if
        the response indicates acknowledgement from the device.

        Args:
            request (str): The request string to send.

        Returns:
            Optional[bytes]: If the response could be decoded,
            then it is returned. Otherwise None is returned.
        """
        with self._lock:
            self._send(request)

            if request.endswith(b"?"):
                try:
                    response = self._socket.recv(RECV_BUFFER)
                    return response
                except UnicodeDecodeError as e:
                    warnings.warn(f"{e}:\n{self._format_message(response).decode()}")
                except socket.timeout:
                    warnings.warn("Didn't receive a response in time.")

        # self._log.debug(f"Received response:\n{self._format_message(decoded_response)}")
        return None

    def send_receive(self, request: bytes) -> Optional[bytes]:
        """Sends a request and attempts to decode the response.

        Args:
            request (str): The request string to send.

        Returns:
            Optional[str]: The decoded response string if the
            request was successful, otherwise None is returned.
        """

        response = self._send_receive(request)
        if response is None:
            return None

        return response
