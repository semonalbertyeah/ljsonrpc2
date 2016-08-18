# -*- coding:utf-8 -*-

import os, sys, time

module_path = os.path.dirname(os.path.abspath('.'))
sys.path.append(module_path)

from zjsonrpc2 import RPCServer

def tprint(msg):
    if not (msg.endswith('\n') or msg.endswith('\r')):
        msg = msg + '\n'
    sys.stdout.write(msg)
    sys.stdout.flush()

def log_err(msg):
    tprint('---- err ----')
    tprint(msg)

def log_info(msg):
    tprint(msg)

def get_server():
    server = RPCServer('tcp://*:9999', timeout=3000, log_info=log_info, log_err=log_err)

    @server.procedure(name='test')
    def test_func():
        return 'test'

    @server.procedure('exception')
    def e_func():
        raise Exception, 'test exception'

    @server.procedure()
    def echo(*args, **kwargs):
        return 'echo: %s, %s' % (str(args), str(kwargs))

    return server


if __name__ == '__main__':
    try:
        with get_server() as server:
            server.start()
            while True:
                time.sleep(0.5)
    except KeyboardInterrupt, e:
        print 'test end'
