# -*- coding:utf-8 -*-

import zmq
import uuid
import traceback
from collections import deque

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




class Queue(object):
    def __init__(self, iterable=(), maxlen=None):
        self._que = deque(iterable)
        self._maxlen = maxlen

    def append(self, v):
        """
            return:
                True -> succeed
                False -> queue is full.
        """
        if len(self._que) < self._maxlen:
            self._que.appendleft(v)
            return True
        return False

    def pop(self):
        return self._que.pop()

    def empty(self):
        return len(self._que) == 0

    def clear(self):
        self._que.clear()


def gen_uuid():
    return uuid.uuid1().get_hex()


class LBServer(object):
    """
        zmq server, with load balancing.
        pattern:
            client(REQ) <--> broker(ROUTER, DEALER) <--> worker(REP)
    """

    # stat machine
    STAT_RUNNING = 'running'    # broker, workers threads are running
    STAT_TERMINATING = 'terminating'    # doing terminate running threads
    STAT_READY = 'ready'        # no running threads, but can be run
    STAT_CLOSED = 'closed'      # no running threads, cannot be run

    # control message
    CTL_MSG_WREADY = b'WREADY'

    _backend_endpoint = 'inproc://zeromq_jsonrpc2_backend'


    def __init__(self, endpoint=None, context=None, timeout=3000, workers=3, log_info=None, log_err=None):
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

        self._log_info = log_info
        self._log_err = log_err

        self._timeout = timeout
        self._worker_num = workers

        self._context = context or zmq.Context()
        self._frontend = self._context.socket(zmq.ROUTER)
        self._backend = self._context.socket(zmq.ROUTER)
        self._backend.bind(self._backend_endpoint)
        if endpoint:
            self.bind(endpoint)

        self._broker_poller = zmq.Poller()
        self._broker_poller.register(self._frontend, zmq.POLLIN)
        self._broker_poller.register(self._backend, zmq.POLLIN)

        self._stat = self.STAT_READY

    def log_info(self, *msg):
        msg = ''.join(msg)
        if self._log_info:
            self._log_info(msg)

    def log_err(self, *msg):
        msg = ''.join(msg)
        if self._log_err:
            self._log_err(msg)
        else:
            print msg   # error message must be shown


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
        try:
            req_que = Queue(maxlen=self._worker_num * 2)
            idle_workers = []
            while self._stat == self.STAT_RUNNING:
                socks = dict(self._broker_poller.poll(self._timeout))

                if socks.get(self._backend, None) == zmq.POLLIN:
                    resp_msg = self._backend.recv_multipart()
                    wid, _, msg = resp_msg[:3]

                    idle_workers.append(wid)
                    if msg != self.CTL_MSG_WREADY and len(resp_msg) > 3:
                        # response
                        cid = msg
                        _, resp = resp_msg[3:]
                        self._frontend.send_multipart([cid, b'', resp])
                    elif msg == self.CTL_MSG_WREADY:
                        self.log_info('worker-%s ready' % wid)

                if socks.get(self._frontend, None) == zmq.POLLIN:
                    req_msg = self._frontend.recv_multipart()
                    if not req_que.append(req_msg):
                        self.log_info('request queue is full, drop request from %s' % repr(req_msg[0]))

                while (not req_que.empty()) and idle_workers:
                    cid, _, req = req_que.pop()
                    wid = idle_workers.pop(0)
                    self._backend.send_multipart([wid, b'', cid, b'', req])

                    # self._backend.send_multipart(msg)

            self.log_info('broker end')
        except Exception, e:
            self.log_err(
                ('%s ERROR in broker:\n' % self.__class__.__name__),
                traceback.format_exc()
            )
            raise


    @threaded(name='worker', start=True, daemon=True)
    def run_worker(self, ident=None):
        """
            handle messages
        """
        try:
            worker_socket = self._context.socket(zmq.REQ)
            worker_socket.identity = ident or gen_uuid()
            worker_socket.connect(self._backend_endpoint)

            # inform broker of worker being ready
            worker_socket.send(self.CTL_MSG_WREADY)

            while self._stat == self.STAT_RUNNING:
                if worker_socket.poll(self._timeout, zmq.POLLIN) == zmq.POLLIN:
                    addr, _, msg = worker_socket.recv_multipart()
                    result = self.handle_one_request(msg)
                    if result is None:
                        result = 'no proper handler for incoming message.'
                        self.log_info('no proper handler, client-%s, msg-%s' % (addr, msg))
                    worker_socket.send_multipart([addr, b'', result])

            self.log_info('worker-%s end' % worker_socket.identity)
            worker_socket.close()
        except Exception, e:
            self.log_err(
                ('%s ERROR in worker' % self.__class__.__name__),
                traceback.format_exc()
            )
            raise


    def start(self):
        assert self._stat == self.STAT_READY, 'cannot run in state: %s' % self._stat
        assert len(self._endpoints) > 0, 'no bound endpoint(s).'

        self._stat = self.STAT_RUNNING

        self.log_info('server starts on %s' % repr(self._endpoints))

        self._broker_task = self.run_broker()
        for i in xrange(self._worker_num):
            ident='worker-%d' % i
            self._worker_tasks.append(self.run_worker(ident))


    # def run(self):
    #     assert self._stat == self.STAT_READY, 'cannot run in state: %s' % self._stat
    #     assert len(self._endpoints) > 0, 'no bound endpoint(s).'

    #     self._stat = self.STAT_RUNNING
    #     for i in xrange(self._worker_num):
    #         self._worker_tasks.append(self.run_worker())

    #     try:
    #         while self._stat == self.STAT_RUNNING:
    #             socks = dict(self._broker_poller.poll(self._timeout))

    #             if socks.get(self._frontend, None) == zmq.POLLIN:
    #                 msg = self._frontend.recv_multipart()
    #                 self._backend.send_multipart(msg)

    #             if socks.get(self._backend, None) == zmq.POLLIN:
    #                 msg = self._backend.recv_multipart()
    #                 self._frontend.send_multipart(msg)
    #     except KeyboardInterrupt, e:
    #         self._stat = self.STAT_TERMINATING
    #         for worker_task in self._worker_tasks:
    #             worker_task.join()
    #         self._worker_tasks = []
    #         self._stat = self.STAT_READY


    def terminate(self):
        if self._stat == self.STAT_RUNNING:
            self._stat = self.STAT_TERMINATING

            self._broker_task.join()
            self._broker_task = None

            for worker_task in self._worker_tasks:
                worker_task.join()
            self._worker_tasks = []

            self._stat = self.STAT_READY

            self.log_info('server terminated.')


    def close(self):
        if getattr(self, '_stat', None) != self.STAT_CLOSED:
            self.terminate()
            self._backend.close()
            self._frontend.close()
            self._stat = self.STAT_CLOSED

            self.log_info('server closed.')


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



class RPCServer(LBServer):
    def __init__(self, endpoint=None, context=None, timeout=3000, workers=3, log_info=None, log_err=None):
        super(RPCServer, self).__init__(endpoint, context, timeout, workers, log_info=log_info, log_err=log_err)
        self.rpc = JSONRPC2Handler(log_info=log_info, log_err=log_err)
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



