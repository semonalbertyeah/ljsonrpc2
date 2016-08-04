# -*- coding:utf-8 -*-

import zmq

from jsonrpc2 import (
    Error, ParseError, InvalidRequest, NotFound,
    InvalidParams, InternalError,
    JSONRPC2Handler,
    Request, Response
)


class Server(object):
    """
        A zmq REP server (care nothing about JSON-RPC2).
        In which some handlers could be added.
    """

    def __init__(self, endpoint=None, context=None, timeout=3000):
        """
            endpoint: zmq endpoint.
            context: instance of zmq.Context.
            timeout: time to wait a new msg. None -> wait forever.
        """
        self._handlers = []
        self._context = context or zmq.Context.instance()
        self._timeout = timeout
        self._endpoints = set()     # bound endpoints
        self._sock = self._context.socket(zmq.REP)
        if endpoint:
            self.bind(endpoint)

    def bind(self, endpoint):
        if endpoint not in self._endpoints:
            self._sock.bind(endpoint)
            self._endpoints.add(endpoint)

    def unbind(self, endpoint):
        if endpoint in self._endpoints:
            self._sock.unbind(endpoint)
            self._endpoints.remove(endpoint)

    def close(self):
        self._sock.close()

    @property
    def closed(self):
        return self._sock.closed

    def add_handler(self, handler):
        """
            add a msg handler.
            format: 
                handler(msg)
                if handler return None -> not handled -> to next handler
        """
        self._handlers.append(handler)

    def handle_one_request(self, timeout=None):
        """
            output:
                bool -> whether a request is handled.
                None -> timeout
        """
        assert not self.closed
        assert len(self._endpoints) > 0

        if timeout is None:
            timeout = self._timeout

        if self._sock.poll(timeout, zmq.POLLIN) == zmq.POLLIN:
            msg = self._sock.recv()
            result = None
            for handler in self._handlers:
                result = handler(msg)
                if result is None:
                    continue
            if result is None:
                self._sock.send('no proper handler for incoming message.')
                return False    # not handled
            else:
                self._sock.send(result)
                return True # handled

        return None # timeout

    def run(self, count=None):
        """
            input:
                count -> how many requests will be handled.
                    None means infinite.
        """
        while (count is None) or (count > 0):
            success = self.handle_one_request()
            if success is not None:
                print 'success' if success else 'failure'
                if count is not None:
                    count -= 1
            else:
                print 'timeout'

    def __del__(self):
        self.close()




class RPCServer(Server):
    """
        function:
            zmq(REP) publisher
    """

    def __init__(self, endpoint=None, context=None, timeout=3000):
        super(RPCServer, self).__init__(endpoint, context, timeout)

        self.rpc = JSONRPC2Handler()
        self.add_handler(self.handle_jsonrpc2)

    def register_function(self, *args, **kwargs):
        return self.rpc.register_function(*args, **kwargs)

    # decorator
    def procedure(self, *args, **kwargs):
        return self.rpc.procedure(*args, **kwargs)

    def handle_jsonrpc2(self, msg):
        if not Request.is_jsonrpc2_request(msg):
            print 'not jsonrpc2 request'
            return None
        else:
            result = self.rpc(msg)
            return '' if result is None else result
