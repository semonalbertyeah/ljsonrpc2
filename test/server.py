# -*- coding:utf-8 -*-

import os, sys

module_path = os.path.dirname(os.path.abspath('.'))
sys.path.append(module_path)

from zjsonrpc2 import RPCServer 


def get_server():
    server = RPCServer('tcp://*:9999', timeout=3000)

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
    with get_server() as server:
        server.run()