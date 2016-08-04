# -*- coding:utf-8 -*-

from ljsonrpc2 import Server 

def case1():
    server = Server('tcp://127.0.0.1:8975')

    # flask-like
    @server.register    # register(array of func | func)
    def test():
        return 'test'

    def test2():
        return 'test2'

    server.register(test2)
    return server

def case2():

    class A(object):
        def test_a(self):
            return 'A.test'

    server.register('tcp://127.0.0.1:8975', target=A())
    return server

def case3():
    # java-like
    class EchoServer(Server):
        def do_echo(self, msg):
            return 'echoed: %s' % msg
    
    return EchoServer('tcp://127.0.0.1:8975')

