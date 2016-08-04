# -*- coding:utf-8 -*-

"""
    JSON-RPC2 specific.
"""

import json
import traceback
import uuid
from pydoc import getdoc


class Error(Exception):
    """
        JSON-RPC2 Error object.
        error code:
            -32700              Parser Error
            -32600              Invalid Request
            -32601              Method not found
            -32602              Invalid params
            -32603              Internal error      (Internal JSON-RPC error)
            -32000 to -32099    Server error
    """
    def __init__(self, code, message, data=None):
        self.code = int(code)
        self.message = str(message)
        self.data = data    # a Primitive or Structured value.
                            # data MAY be ommited.
        # TODO: log error here

    def encode(self):
        """
            to dict
        """
        msg = {'code': self.code, 'message': self.message}
        if self.data is not None:
            msg['data'] = self.data

        return msg

    @staticmethod
    def decode(msg):
        """
            from dict
        """
        if isinstance(msg, Error):
            return msg
        if isinstance(msg, (str, unicode)):
            msg = json.loads(msg)

        return Error(**msg)

    def __unicode__(self):
        return ('%d: %s' % (self.code, self.message)).decode('utf-8')

    def __str__(self):
        return self.__unicode__().encode('utf-8')


class ParseError(Error):
    def __init__(self, message='Invalid JSON', data=None):
        super(ParseError, self).__init__(-32700, message, data)


class InvalidRequest(Error):
    def __init__(self, message='Invalid Request', data=None):
        super(InvalidRequest, self).__init__(-32600, message, data)


class NotFound(Error):
    def __init__(self, message='Method not found', data=None):
        super(NotFound, self).__init__(-32601, message, data)


class InvalidParams(Error):
    def __init__(self, message='Invalid parameters', data=None):
        super(InvalidParams, self).__init__(-32602, message, data)


class InternalError(Error):
    def __init__(self, message='Internal error', data=None):
        super(InternalError, self).__init__(-32603, message, data)



class Request(object):
    """
        JSON-RPC2 Request object.
    """
    def __init__(self, method, params=None, id=None):
        self.jsonrpc = '2.0'    # MUST be '2.0'
        self.method = str(method)
        self.params = params    # MAY be omitted

        self.id = id            # if not present, request is a notification(2.0).
                                # in JSON-RPC1.0, id=null means request is a notification.
                                # Both 1.0 and 2.0 are handled here, and treated as 2.0 Notification.

    @property
    def is_notification(self):
        return self.id is None

    @staticmethod
    def generate_id():
        return str(uuid.uuid4())

    def encode(self):
        """
            to string (for sending it to server)
        """
        msg = {'method': self.method, 'jsonrpc': self.jsonrpc}
        if self.params is not None:
            msg['params'] = self.params
        if self.id is not None:
            msg['id'] = self.id

        return json.dumps(msg)

    @staticmethod
    def decode(msg):
        """
            parse a request
        """
        if isinstance(msg, Request):
            return msg
        try:
            if not isinstance(msg, dict):
                msg = json.loads(msg)
            if msg.pop('jsonrpc', None) != '2.0':
                raise InvalidRequest(message='Only JSON-RPC2.0 requests are accepted.')
            if msg.has_key('id') and msg['id'] is None:
                raise InvalidRequest(message='1.0 notification is not supported.')
            return Request(**msg)
        except ValueError, e:
            # json.loads failed
            raise ParseError()
        except TypeError, e:
            # parameters not match Request
            raise InvalidRequest()

    @staticmethod
    def is_jsonrpc2_request(req):
        """
            check if request is a 
        """
        try:
            Request.decode(req)
        except (ParseError, InvalidRequest) as e:
            return False
        else:
            return True


class Response(object):
    """
        JSON-RPC2 Response object.
    """
    def __init__(self, id, result=None, error=None):
        # either 'result' or 'error' MUST be included, but not both.
        assert (result is None and error is not None) or (result is not None and error is None)
        self.jsonrpc = '2.0'    # MUST
        self.id = id
        self.result = result
        self.error = Error.decode(error) if error is not None else None

    @property
    def is_error(self):
        return self.error is not None

    def encode(self):
        msg = {'id': self.id, 'jsonrpc': self.jsonrpc}
        if self.result is not None:
            msg['result'] = self.result
        elif self.error is not None:
            msg['error'] = self.error.encode()

        return json.dumps(msg)

    @staticmethod
    def decode(msg):
        if isinstance(msg, Response):
            return msg

        if isinstance(msg, (str, unicode)):
            msg = json.loads(msg)

        assert msg.pop('jsonrpc', None) == '2.0'
        return Response(**msg)


class JSONRPC2Handler(object):
    """
        handle a JSON-RPC2 request.
    """
    def __init__(self, introspected=True):
        self._methods = {}
        if introspected:
            self.register_introspection_functions()


    #########################
    # introspected functions
    #########################

    def list_methods(self):
        return self._methods.keys()

    def method_doc(self, method_name):
        func = self._methods.get(method_name, None)
        if func is None:
            return ''
        else:
            return getdoc(func)

    def register_introspection_functions(self):
        self._methods.update({
                'list_methods': self.list_methods,
                'method_doc': self.method_doc
            })

    def register_function(self, func, name=None):
        assert callable(func)
        name = name or func.__name__
        self._methods[name] = func

    # decorator
    def procedure(self, name=None):
        """
            register a function
        """
        def decorator(func):
            self.register_function(func, name)
            return func
        return decorator

    def make_response(self, req_id, content):
        """
            input:
                req_id -> request id, could be None if failed to dectect id in request.
                content -> Error or result
            output:
                instance of Response
        """
        result = error = None
        if isinstance(content, Error):
            error = content
        else:
            result = content
        return Response(req_id, result, error)


    def handle_one_request(self, msg):
        """
            input:
                raw request
            output:
                raw response or None
        """
        req_id = None
        try:
            req = Request.decode(msg)
            req_id = req.id
            if not self._methods.has_key(req.method):
                raise NotFound(message='No such method: %s.' % req.method)

            func = self._methods[req.method]
            params = req.params
            try:
                if params is None:
                    result = func()
                elif isinstance(params, list):
                    result = func(*params)
                elif isinstance(params, dict):
                    result = func(**params)
                else:
                    raise InvalidParams(message="invalid parameter %s, method: %s" % (json.dumps(params), req.method))

                if req_id is None:
                    # notification
                    return None
                else:
                    return self.make_response(req_id, result).encode()
            except TypeError, e:
                # parameters don't match function
                raise InvalidParams(message="parameter - %s not match %s" % (json.dumps(params), req.method))
            # except Exception, e:
            #     raise InternalError(message="Internal Error.", data={'traceback': traceback.format_exc()})

        except Error, e:
            print 'Error:', str(e)
            return self.make_response(req_id, e).encode()

        except Exception, e:
            print 'Exception:', str(e)
            return self.make_response(
                    req_id,
                    InternalError(message='InternalError', data={'traceback': traceback.format_exc()})
                ).encode()

    def __call__(self, msg):
        """
            to handle JSON-RPC2 Request
            input:
                msg -> raw string message
            output:
                JSON-RPC2 response (string)
                '' for notification
        """
        return self.handle_one_request(msg)





if __name__ == '__main__':
    req = Request('list_methods', id=1)

    rpc = JSONRPC2Handler()
    print rpc(req.encode())
