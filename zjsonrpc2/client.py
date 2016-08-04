# -*- coding:utf-8 -*-

import zmq

from jsonrpc2 import (
    Error, ParseError, InvalidRequest, NotFound,
    InvalidParams, InternalError,
    Request, Response
)


class RPCMethod(object):
    def __init__(self, method, rpc_client):
        self.method = method
        self.rpc_client = rpc_client

    def __call__(self, *args, **kwargs):
        """
            RPC request.
        """
        params = None
        if args:
            params = list(args)
        elif kwargs:
            params = kwargs

        req = Request(self.method, params, id=Request.generate_id())
        resp = self.rpc_client.do_request(req)
        return resp.result

    def notify(self, *args, **kwargs):
        """
            RPC notification.
        """
        params = None
        if args:
            params = list(args)
        elif kwargs:
            params = kwargs

        req = Request(self.method, params, id=None)
        self.rpc_client.do_request(req)


class RPCClient(object):
    """
        client
        support:
            with keyword
    """

    def __init__(self, endpoint=None, context=None, timeout=3000):
        self._endpoints = set()
        self._timeout = timeout
        self._context = context or zmq.Context.instance()
        self._sock = self._context.socket(zmq.REQ)
        if endpoint:
            self.connect(endpoint)

    def connect(self, endpoint):
        if endpoint not in self._endpoints:
            self._sock.connect(endpoint)
            self._endpoints.add(endpoint)

    def disconnect(self, endpoint):
        if endpoint in self._endpoints:
            self._sock.disconnect(endpoint)
            self._endpoints.remove(endpoint)

    def close(self):
        self._sock.close()

    @property
    def closed(self):
        return self._sock.closed

    def do_request(self, req):
        """
            input:
                req -> Request
            output:
                Response
                or
                None (notification)
        """
        if not isinstance(req, Request):
            req = Request.decode(req)

        assert len(self._endpoints) > 0
        assert not self.closed

        self._sock.send(req.encode())
        if self._sock.poll(self._timeout, zmq.POLLIN) == zmq.POLLIN:
            resp = self._sock.recv()    # IMP: sock.poll(timeout, POLLIN) -> sock.recv(NOBLOCK) # will raise EAGAIN if no response received.
            if req.is_notification:
                return None
            else:
                resp = Response.decode(resp)
                if resp.is_error:
                    raise resp.error

                assert resp.id == req.id
                return resp
        else:
            raise zmq.ZMQError(errno=zmq.EAGAIN, msg='timeout when receiving response.')


    # def __call__(self, method, *args, **kwargs):
    #     """
    #         call RPC on remote Server.
    #         return:
    #             RPC result
    #     """
    #     if args:
    #         params = list(args)
    #     elif kwargs:
    #         params = kwargs
    #     else:
    #         params = None
    #     return Request(method, params=params, id=Request.generate_id()).encode()

    # def notify(self, method, *args, **kwargs):
    #     """
    #         call RPC on remote server, without returned value.
    #     """
    #     if args:
    #         params = list(args)
    #     elif kwargs:
    #         params = kwargs
    #     else:
    #         params = None
    #     return Request(method, params=params, id=None).encode()

    def __getattr__(self, method):
        """
            return a callable, which will call RPC.
        """
        return RPCMethod(method, self)

    def __enter__(self):
        assert len(self._endpoints) > 0 , 'no endpoint'
        assert not self.closed , 'closed'
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def __del__(self):
        self.close()

