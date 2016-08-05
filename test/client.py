# -*- coding:utf-8 -*-

import os, sys

module_path = os.path.dirname(os.path.abspath('.'))
sys.path.append(module_path)

from zjsonrpc2 import RPCClient, Error


if __name__ == '__main__':
    with RPCClient('tcp://127.0.0.1:9999', timeout=3000) as client:
        methods = client.list_methods()
        print 'methods:', methods

        print 'test_func:', client.test()
        print 'echo', client.echo('echo')

        try:
            client.exception()
        except Error, e:
            print 'RPC Error:', str(e)
