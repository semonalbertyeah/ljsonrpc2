# -*- coding:utf-8 -*-


from ljsonrpc2 import Client

client = Client('tcp://127.0.0.1:8975')
print client.test()
