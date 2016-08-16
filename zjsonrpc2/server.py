# -*- coding:utf-8 -*-

import zmq

from .thread_util import threaded
from .jsonrpc2 import (
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
                handler(msg) -> should return a raw message (string)
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

    def __enter__(self):
        assert len(self._endpoints) > 0
        assert not self.closed

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()


class LBServer(object):
    """
        zmq server, with load balancing.
        pattern:
            client(REQ) <--> broker(ROUTER, DEALER) <--> worker(REP)
    """

    STAT_RUNNING = 'running'    # broker, workers threads are running
    STAT_TERMINATING = 'terminating'    # doing terminate running threads
    STAT_READY = 'ready'        # no running threads, but can be run
    STAT_CLOSED = 'closed'      # no running threads, cannot be run

    _backend_indent = 'zjsonrpc2_server_backend'


    def __init__(self, endpoint=None, context=None, timeout=3000, workers=3):
        """
            endpoint: zmq endpoint.
            context: instance of zmq.Context
            timeout: 
                time to wait a new msg (for both worker and broker).
                None -> wait forever.
        """

        self._handlers = []             # message handlers
        self._endpoints = set()     # bound endpoints
        self._broker_task = None
        self._worker_tasks = []

        self._timeout = timeout
        self._worker_num = workers

        self._context = context or zmq.Context.instance()
        self._frontend = self._context.socket(zmq.ROUTER)
        self._backend = self._context.socket(zmq.DEALER)
        self._backend.bind('inproc://%s' % self._backend_indent)
        if endpoint:
            self.bind(endpoint)

        self._broker_poller = zmq.Poller()
        self._broker_poller.register(self._frontend, zmq.POLLIN)
        self._broker_poller.register(self._backend, zmq.POLLIN)

        self._stat = self.STAT_READY


    def bind(self, endpoint):
        if endpoint not in self._endpoints:
            self._frontend.bind(endpoint)
            self._endpoints.add(endpoint)

    def unbind(self, endpoint):
        if endpoint in self._endpoints:
            self._frontend.unbind(endpoint)
            self._endpoints.remove(endpoint)


    def add_handler(self, handler):
        """
            add a msg handler.
            format: 
                handler(msg) -> should return a raw message (string).
                if handler return None -> not handled -> to next handler
        """
        self._handlers.append(handler)


    def handle_one_request(self, msg):
        """
            apply msg to every handlers until got one result (not None).
            output:
                result(not None) -> result of msg handler
                None -> not proper handler found
        """
        for handler in self._handlers:
            result = handler(msg)
            if result is not None:
                return result

        return None


    @threaded(name='broker', start=True, daemon=True)
    def run_broker(self):
        """
            forward messages between workers and clients.
        """
        while self._stat == self.STAT_RUNNING:
            socks = dict(self._broker_poller.poll(self._timeout))

            if socks.get(self._frontend, None) == zmq.POLLIN:
                msg = self._frontend.recv_multipart()
                self._backend.send_multipart(msg)

            if socks.get(self._backend, None) == zmq.POLLIN:
                msg = self._backend.recv_multipart()
                self._frontend.send_multipart(msg)


    @threaded(name='worker', start=True, daemon=True)
    def run_worker(self):
        """
            handle messages
        """
        worker_socket = self._context.socket(zmq.REP)
        worker_socket.connect('inproc://%s' % self._backend_indent)

        while self._stat == self.STAT_RUNNING:
            if worker_socket.poll(self._timeout, zmq.POLLIN) == zmq.POLLIN:
                msg = worker_socket.recv()
                result = self.handle_one_request(msg)
                if result is not None:
                    worker_socket.send(result)
                else:
                    worker_socket.send('no proper handler for incoming message.')

        worker_socket.close()


    def start(self):
        assert self._stat == self.STAT_READY, 'cannot run in state: %s' % self._stat
        assert len(self._endpoints) > 0, 'no bound endpoint(s).'

        self._stat = self.STAT_RUNNING

        self._broker_task = self.run_broker()
        for i in xrange(self._worker_num):
            self._worker_tasks.append(self.run_worker())

    def run(self):
        assert self._stat == self.STAT_READY, 'cannot run in state: %s' % self._stat
        assert len(self._endpoints) > 0, 'no bound endpoint(s).'

        self._stat = self.STAT_RUNNING
        for i in xrange(self._worker_num):
            self._worker_tasks.append(self.run_worker())

        try:
            while self._stat == self.STAT_RUNNING:
                socks = dict(self._broker_poller.poll(self._timeout))

                if socks.get(self._frontend, None) == zmq.POLLIN:
                    msg = self._frontend.recv_multipart()
                    self._backend.send_multipart(msg)

                if socks.get(self._backend, None) == zmq.POLLIN:
                    msg = self._backend.recv_multipart()
                    self._frontend.send_multipart(msg)
        except KeyboardInterrupt, e:
            self._stat = self.STAT_TERMINATING
            for worker_task in self._worker_tasks:
                worker_task.join()
            self._worker_tasks = []
            self._stat = self.STAT_READY



    def terminate(self):
        if self._stat == self.STAT_RUNNING:
            self._stat = self.STAT_TERMINATING

            self._broker_task.join()
            self._broker_task = None

            for worker_task in self._worker_tasks:
                worker_task.join()
            self._worker_tasks = []

            self._stat = self.STAT_READY


    def close(self):
        if getattr(self, '_stat', None) != self.STAT_CLOSED:
            self.terminate()
            self._backend.close()
            self._frontend.close()
            self._stat = self.STAT_CLOSED


    # expose state
    @property
    def closed(self):
        return self._stat == self.STAT_CLOSED

    @property
    def ready(self):
        return self._stat == self.STAT_READY

    @property
    def running(self):
        return self._stat == self.STAT_RUNNING

    @property
    def terminating(self):
        return self._stat == self.STAT_TERMINATING

    def __enter__(self):
        assert self._stat == self.STAT_READY

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()



class RPCServer(Server):
    """
        zmq(REP) publisher
    """

    def __init__(self, endpoint=None, context=None, timeout=3000, logger=None):
        super(RPCServer, self).__init__(endpoint, context, timeout)

        self.rpc = JSONRPC2Handler(logger=logger)
        self.add_handler(self.handle_jsonrpc2)

    def register_function(self, *args, **kwargs):
        return self.rpc.register_function(*args, **kwargs)

    # decorator
    def procedure(self, *args, **kwargs):
        return self.rpc.procedure(*args, **kwargs)

    def handle_jsonrpc2(self, msg):
        if not Request.is_jsonrpc2_request(msg):
            return None
        else:
            result = self.rpc(msg)
            return '' if result is None else result

class LBRPCServer(LBServer):
    def __init__(self, endpoint=None, context=None, timeout=3000, workers=3, logger=None):
        super(LBRPCServer, self).__init__(endpoint, context, timeout, workers)
        self.rpc = JSONRPC2Handler(logger=logger)
        self.add_handler(self.handle_jsonrpc2)

    def register_function(self, *args, **kwargs):
        return self.rpc.register_function(*args, **kwargs)

    # decorator
    def procedure(self, *args, **kwargs):
        return self.rpc.procedure(*args, **kwargs)

    def handle_jsonrpc2(self, msg):
        if not Request.is_jsonrpc2_request(msg):
            return None
        else:
            result = self.rpc(msg)
            return '' if result is None else result # None means notification
