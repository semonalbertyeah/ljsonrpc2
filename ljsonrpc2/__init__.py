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
            zmq
            jsonrpc2
    """
    def __init__(self, endpoint=None, context=None):
        """
            endpoint: zmq endpoint.
            context: instance of zmq.Context.
        """
        self._context = context or Context.get_instance()
        self._sock = self._context.socket(zmq.REP)
        self._sock.bind(endpoint)

    @staticmethod
    def is_jsonrpc2_request(req):
        """
            check if request is a 
        """
        if isinstance(req, (str, unicode)):
            req = json.loads(req)

        assert isinstance(req, dict)

        if req.get('jsonrpc', None) != '2.0':
            return False

        if not isinstance(req.get('method', None), (str, unicode)):
            return False

        if req.has_key('params'):   # params MAY be omitted.
            if not isinstance(req['params'], (list, dict)): # params MUST be a structured value (Object or Array).
                return False

    @staticmethod
    def is_jsonrpc2_notification(req):

    def bind(self, endpoint):
        self._sock.bind(endpoint)


    def recv(self):
        self._sock.recv_json()

    def handle_one_request(self):
        req = self.recv()

    def run(self):
        msg = self.recv()


class Client(object):
    pass

