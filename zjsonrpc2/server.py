# -*- coding:utf-8 -*-

import zmq
import json

class Context(zmq.Context):
    _instance = None    # there should be only 1 context per process.

    @staticmethod
    def get_instance():
        if Context._instance is None:
            Context._instance = Context()
        return Context._instance




class Server(object):
    """
        function:
            zmq(REP) publisher
    """
    def __init__(self, endpoint=None, context=None):
        """
            endpoint: zmq endpoint.
            context: instance of zmq.Context.
        """
        self._context = context or Context.get_instance()
        self._sock = self._context.socket(zmq.REP)
        self._sock.bind(endpoint)


    def add_subscriber(sub):
        """
            sub: 
                a callable -> sub(raw_msg)
        """
        pass


    def bind(self, endpoint):
        self._sock.bind(endpoint)


    def recv(self):
        self._sock.recv_json()

    def handle_one_request(self):
        req = self.recv()

    def run(self):
        msg = self.recv()


class Client(object):
    """
        RPC proxy
    """
    pass

