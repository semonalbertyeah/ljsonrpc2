# -*- coding:utf-8 -*-

import os, sys, time

module_path = os.path.dirname(os.path.abspath('.'))
sys.path.append(module_path)

from zjsonrpc2 import LBRPCServer 


def get_server():
    server = LBRPCServer('tcp://*:9999', timeout=3000)

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
        print 'end'