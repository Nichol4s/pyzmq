"""Python bindings for 0MQ."""

#
#    Copyright (c) 2010 Brian E. Granger
#
#    This file is part of pyzmq.
#
#    pyzmq is free software; you can redistribute it and/or modify it under
#    the terms of the Lesser GNU General Public License as published by
#    the Free Software Foundation; either version 3 of the License, or
#    (at your option) any later version.
#
#    pyzmq is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    Lesser GNU General Public License for more details.
#
#    You should have received a copy of the Lesser GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------
import cython

from stdlib cimport *
from python_string cimport PyString_FromStringAndSize
from python_string cimport PyString_AsStringAndSize
from python_string cimport PyString_AsString, PyString_Size
from python_bytes cimport PyBytes_AsStringAndSize

cdef extern from "Python.h":
    ctypedef int Py_ssize_t
    cdef void Py_INCREF( object o )
    cdef void Py_DECREF( object o )
    cdef void PyEval_InitThreads()

PyEval_InitThreads()

import cPickle as pickle
import random
import struct


try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json = None

include "allocate.pxi"

#-----------------------------------------------------------------------------
# Import the C header files
#-----------------------------------------------------------------------------

cdef extern from "errno.h" nogil:
    enum: ZMQ_EINVAL "EINVAL"
    enum: ZMQ_EAGAIN "EAGAIN"

cdef extern from "string.h" nogil:
    void *memcpy(void *dest, void *src, size_t n)
    size_t strlen(char *s)

cdef extern from "zmq_compat.h":
    ctypedef signed long long int64_t "pyzmq_int64_t"

# cdef extern from *:
#     # This isn't necessarily a signed long long, but this will let Cython
#     # get it right.
#     ctypedef signed long long int64_t

cdef extern from "zmq.h" nogil:
    enum: ZMQ_HAUSNUMERO
    enum: ZMQ_ENOTSUP "ENOTSUP"
    enum: ZMQ_EPROTONOSUPPORT "EPROTONOSUPPORT"
    enum: ZMQ_ENOBUFS "ENOBUFS"
    enum: ZMQ_ENETDOWN "ENETDOWN"
    enum: ZMQ_EADDRINUSE "EADDRINUSE"
    enum: ZMQ_EADDRNOTAVAIL "EADDRNOTAVAIL"
    enum: ZMQ_ECONNREFUSED "ECONNREFUSED"
    enum: ZMQ_EINPROGRESS "EINPROGRESS"
    enum: ZMQ_EMTHREAD "EMTHREAD"
    enum: ZMQ_EFSM "EFSM"
    enum: ZMQ_ENOCOMPATPROTO "ENOCOMPATPROTO"
    enum: ZMQ_ETERM "ETERM"

    enum: errno
    char *zmq_strerror (int errnum)
    int zmq_errno()

    enum: ZMQ_MAX_VSM_SIZE # 30
    enum: ZMQ_DELIMITER # 31
    enum: ZMQ_VSM # 32

    ctypedef struct zmq_msg_t:
        void *content
        unsigned char shared
        unsigned char vsm_size
        unsigned char vsm_data [ZMQ_MAX_VSM_SIZE]
    
    ctypedef void zmq_free_fn(void *data, void *hint)
    
    int zmq_msg_init (zmq_msg_t *msg)
    int zmq_msg_init_size (zmq_msg_t *msg, size_t size)
    int zmq_msg_init_data (zmq_msg_t *msg, void *data,
        size_t size, zmq_free_fn *ffn, void *hint)
    int zmq_msg_close (zmq_msg_t *msg)
    int zmq_msg_move (zmq_msg_t *dest, zmq_msg_t *src)
    int zmq_msg_copy (zmq_msg_t *dest, zmq_msg_t *src)
    void *zmq_msg_data (zmq_msg_t *msg)
    size_t zmq_msg_size (zmq_msg_t *msg)
    
    enum: ZMQ_POLL # 1

    void *zmq_init (int app_threads, int io_threads, int flags)
    int zmq_term (void *context)

    enum: ZMQ_P2P # 0, deprecated, use ZMQ_PAIR
    enum: ZMQ_PAIR # 0
    enum: ZMQ_PUB # 1
    enum: ZMQ_SUB # 2
    enum: ZMQ_REQ # 3
    enum: ZMQ_REP # 4
    enum: ZMQ_XREQ # 5
    enum: ZMQ_XREP # 6
    enum: ZMQ_UPSTREAM # 7
    enum: ZMQ_DOWNSTREAM # 8

    enum: ZMQ_HWM # 1
    enum: ZMQ_LWM # 2
    enum: ZMQ_SWAP # 3
    enum: ZMQ_AFFINITY # 4
    enum: ZMQ_IDENTITY # 5
    enum: ZMQ_SUBSCRIBE # 6
    enum: ZMQ_UNSUBSCRIBE # 7
    enum: ZMQ_RATE # 8
    enum: ZMQ_RECOVERY_IVL # 9
    enum: ZMQ_MCAST_LOOP # 10
    enum: ZMQ_SNDBUF # 11
    enum: ZMQ_RCVBUF # 12
    enum: ZMQ_RCVMORE # 13

    enum: ZMQ_NOBLOCK # 1
    enum: ZMQ_SNDMORE # 2

    void *zmq_socket (void *context, int type)
    int zmq_close (void *s)
    int zmq_setsockopt (void *s, int option, void *optval, size_t optvallen)
    int zmq_getsockopt (void *s, int option, void *optval, size_t *optvallen)
    int zmq_bind (void *s, char *addr)
    int zmq_connect (void *s, char *addr)
    int zmq_send (void *s, zmq_msg_t *msg, int flags)
    int zmq_recv (void *s, zmq_msg_t *msg, int flags)
    
    enum: ZMQ_POLLIN # 1
    enum: ZMQ_POLLOUT # 2
    enum: ZMQ_POLLERR # 4

    ctypedef struct zmq_pollitem_t:
        void *socket
        int fd
        # #if defined _WIN32
        #     SOCKET fd;
        short events
        short revents

    int zmq_poll (zmq_pollitem_t *items, int nitems, long timeout)


#-----------------------------------------------------------------------------
# Python module level constants
#-----------------------------------------------------------------------------

NOBLOCK = ZMQ_NOBLOCK
PAIR = ZMQ_PAIR
P2P = ZMQ_P2P  # Deprecated, use PAIR
PUB = ZMQ_PUB
SUB = ZMQ_SUB
REQ = ZMQ_REQ
REP = ZMQ_REP
XREQ = ZMQ_XREQ
XREP = ZMQ_XREP
UPSTREAM = ZMQ_UPSTREAM
DOWNSTREAM = ZMQ_DOWNSTREAM
HWM = ZMQ_HWM
LWM = ZMQ_LWM
SWAP = ZMQ_SWAP
AFFINITY = ZMQ_AFFINITY
IDENTITY = ZMQ_IDENTITY
SUBSCRIBE = ZMQ_SUBSCRIBE
UNSUBSCRIBE = ZMQ_UNSUBSCRIBE
RATE = ZMQ_RATE
RECOVERY_IVL = ZMQ_RECOVERY_IVL
MCAST_LOOP = ZMQ_MCAST_LOOP
SNDBUF = ZMQ_SNDBUF
RCVBUF = ZMQ_RCVBUF
RCVMORE = ZMQ_RCVMORE
SNDMORE = ZMQ_SNDMORE
POLL = ZMQ_POLL
POLLIN = ZMQ_POLLIN
POLLOUT = ZMQ_POLLOUT
POLLERR = ZMQ_POLLERR

#-----------------------------------------------------------------------------
# Error handling
#-----------------------------------------------------------------------------

# Often used (these are alse in errno.)
EAGAIN = ZMQ_EAGAIN
EINVAL = ZMQ_EINVAL

# For Windows compatability
ENOTSUP = ZMQ_ENOTSUP
EPROTONOSUPPORT = ZMQ_EPROTONOSUPPORT
ENOBUFS = ZMQ_ENOBUFS
ENETDOWN = ZMQ_ENETDOWN
EADDRINUSE = ZMQ_EADDRINUSE
EADDRNOTAVAIL = ZMQ_EADDRNOTAVAIL
ECONNREFUSED = ZMQ_ECONNREFUSED
EINPROGRESS = ZMQ_EINPROGRESS

# 0MQ Native
EMTHREAD = ZMQ_EMTHREAD
EFSM = ZMQ_EFSM
ENOCOMPATPROTO = ZMQ_ENOCOMPATPROTO
ETERM = ZMQ_ETERM


def strerror(errnum):
    """Return the error string given the error number."""
    return zmq_strerror(errnum)

class ZMQError(Exception):
    """Base exception class for 0MQ errors in Python."""

    def __init__(self, error=None):
        if error is None:
            error = zmq_errno()
        if type(error) == int:
            self.errstr = strerror(error)
            self.errno = error
        else:
            self.errstr = str(error)
            self.errno = None 

    def __str__(self):
        return self.errstr

#-----------------------------------------------------------------------------
# Code
#-----------------------------------------------------------------------------

cdef void decref(void *data, void  *obj) with gil:
    Py_DECREF(<object>obj)

cdef class Message:
    """ A message object
    """

    cdef void *hint
    cdef zmq_free_fn *callback 
    cdef zmq_msg_t zmq_msg
    cdef char *data
    cdef object datao
    cdef object user_callback
    cdef Py_ssize_t data_len
    cdef bool contains_data

    def __cinit__(self, object data=None, object usercb=None):
        cdef int rc
        self.contains_data = False
        self.user_callback = usercb
        if data is None:
            # Initialize empty object
            #Py_INCREF(self)   #XXX
            with nogil:
                rc = zmq_msg_init(&self.zmq_msg)
            if rc != 0:
                raise ZMQError(zmq_errno())
        else:
            rc = PyBytes_AsStringAndSize(data, &self.data, &self.data_len)
            if rc == -1:
                raise TypeError("Object does not provide ByteArray interface")

            # Object spcefied set up pointer and callback
            Py_INCREF(self)
            self.hint = <void *>self
            self.callback = <zmq_free_fn*>decref
            #self.data = <char*>data  # Keep the data alive
            self.datao = data        # Keep the object alive

            self.contains_data = True
            with nogil:
                rc = zmq_msg_init_data(&self.zmq_msg, self.data, self.data_len, self.callback, self.hint)

            if rc != 0:
                self.contains_data = False
                raise ZMQError(zmq_errno())

    def size(self):
        return zmq_msg_size(&self.zmq_msg)

    cdef init_zmq_msg(self, zmq_msg_t new_zmq_msg):
        self.zmq_msg = new_zmq_msg

    def copy(self, Message omsg):
        cdef Message m
        # return a copy of this message
        m = Message()
        Py_INCREF(m)
        zmq_msg_copy(&m.zmq_msg, &omsg.zmq_msg)
        return m

    def __len__(self):
        return <int>zmq_msg_size(&self.zmq_msg)

    def __str__(self):
        cdef void * ptr
        datastr = <char *>zmq_msg_data(&self.zmq_msg)
        dataln = <int>zmq_msg_size(&self.zmq_msg)
        return PyString_FromStringAndSize(datastr, dataln)

    def __dealloc__(self):
        if self.user_callback:
            self.user_callback()

    def close(self):
        rc = zmq_msg_close(&self.zmq_msg)
        if rc != 0:
            raise ZMQError(zmq_errno())


cdef class Context:
    """Manage the lifecycle of a 0MQ context.

    Context(app_threads=1, io_threads=1, flags=0)

    Parameters
    ----------
    app_threads : int
        The number of application threads.
    io_threads : int
        The number of IO threads.
    flags : int
        Any of the Context flags.  Use zmq.POLL to put all sockets into
        non blocking mode and use poll.
    """

    cdef void *handle

    def __cinit__(self, int app_threads=1, int io_threads=1, int flags=0):
        self.handle = NULL
        self.handle = zmq_init(app_threads, io_threads, flags)
        if self.handle == NULL:
            raise ZMQError()

    def __dealloc__(self):
        cdef int rc
        if self.handle != NULL:
            rc = zmq_term(self.handle)
            if rc != 0:
                raise ZMQError()

    def socket(self, int socket_type):
        """Create a Socket associated with this Context.

        Parameters
        ----------
        socket_type : int
            The socket type, which can be any of the 0MQ socket types: 
            REQ, REP, PUB, SUB, PAIR, XREQ, XREP, UPSTREAM, DOWNSTREAM.
        """
        return Socket(self, socket_type)


cdef class Socket:
    """A 0MQ socket.

    Socket(context, socket_type)

    Parameters
    ----------
    context : Context
        The 0MQ Context this Socket belongs to.
    socket_type : int
        The socket type, which can be any of the 0MQ socket types: 
        REQ, REP, PUB, SUB, PAIR, XREQ, XREP, UPSTREAM, DOWNSTREAM.
    """

    cdef void *handle
    cdef public int socket_type
    # Hold on to a reference to the context to make sure it is not garbage
    # collected until the socket it done with it.
    cdef public Context context
    cdef public object closed

    def __cinit__(self, Context context, int socket_type):
        self.handle = NULL
        self.context = context
        self.socket_type = socket_type
        self.handle = zmq_socket(context.handle, socket_type)
        if self.handle == NULL:
            raise ZMQError()
        self.closed = False

    def __dealloc__(self):
        self.close()

    def close(self):
        """Close the socket.

        This can be called to close the socket by hand. If this is not
        called, the socket will automatically be closed when it is
        garbage collected.
        """
        cdef int rc
        if self.handle != NULL and not self.closed:
            rc = zmq_close(self.handle)
            if rc != 0:
                raise ZMQError()
            self.handle = NULL
            self.closed = True

    def _check_closed(self):
        if self.closed:
            raise ZMQError("Cannot complete operation, Socket is closed.")

    def setsockopt(self, int option, optval):
        """Set socket options.

        See the 0MQ documentation for details on specific options.

        Parameters
        ----------
        option : str
            The name of the option to set. Can be any of: SUBSCRIBE, 
            UNSUBSCRIBE, IDENTITY, HWM, LWM, SWAP, AFFINITY, RATE, 
            RECOVERY_IVL, MCAST_LOOP, SNDBUF, RCVBUF.
        optval : int or str
            The value of the option to set.
        """
        cdef int64_t optval_int_c
        cdef int rc

        self._check_closed()

        if option in [SUBSCRIBE, UNSUBSCRIBE, IDENTITY]:
            if not isinstance(optval, str):
                raise TypeError('expected str, got: %r' % optval)
            rc = zmq_setsockopt(
                self.handle, option,
                PyString_AsString(optval), PyString_Size(optval)
            )
        elif option in [HWM, LWM, SWAP, AFFINITY, RATE, RECOVERY_IVL,
                        MCAST_LOOP, SNDBUF, RCVBUF]:
            if not isinstance(optval, int):
                raise TypeError('expected int, got: %r' % optval)
            optval_int_c = optval
            rc = zmq_setsockopt(
                self.handle, option,
                &optval_int_c, sizeof(int64_t)
            )
        else:
            raise ZMQError(EINVAL)

        if rc != 0:
            raise ZMQError()

    def getsockopt(self, int option):
        """Get the value of a socket option.

        See the 0MQ documentation for details on specific options.

        Parameters
        ----------
        option : str
            The name of the option to set. Can be any of: 
            IDENTITY, HWM, LWM, SWAP, AFFINITY, RATE, 
            RECOVERY_IVL, MCAST_LOOP, SNDBUF, RCVBUF, RCVMORE.

        Returns
        -------
        The value of the option as a string or int.
        """
        cdef int64_t optval_int_c
        cdef char identity_str_c [255]
        cdef size_t sz
        cdef int rc

        self._check_closed()

        if option in [IDENTITY]:
            sz = 255
            rc = zmq_getsockopt(self.handle, option, <void *>identity_str_c, &sz)
            if rc != 0:
                raise ZMQError()
            result = PyString_FromStringAndSize(<char *>identity_str_c, sz)
        elif option in [HWM, LWM, SWAP, AFFINITY, RATE, RECOVERY_IVL,
                        MCAST_LOOP, SNDBUF, RCVBUF, RCVMORE]:
            sz = sizeof(int64_t)
            rc = zmq_getsockopt(self.handle, option, <void *>&optval_int_c, &sz)
            if rc != 0:
                raise ZMQError()
            result = optval_int_c
        else:
            raise ZMQError()

        return result

    def bind(self, addr):
        """Bind the socket to an address.

        This causes the socket to listen on a network port. Sockets on the
        other side of this connection will use :meth:`Sockiet.connect` to
        connect to this socket.

        Parameters
        ----------
        addr : str
            The address string. This has the form 'protocol://interface:port',
            for example 'tcp://127.0.0.1:5555'. Protocols supported are
            tcp, upd, pgm, iproc and ipc.
        """
        cdef int rc

        self._check_closed()

        if not isinstance(addr, str):
            raise TypeError('expected str, got: %r' % addr)
        rc = zmq_bind(self.handle, addr)
        if rc != 0:
            raise ZMQError()

    def bind_to_random_port(self, addr, min_port=2000, max_port=20000, max_tries=100):
        """Bind this socket to a random port in a range.

        Parameters
        ----------
        addr : str
            The address string without the port to pass to :meth:`Socket.bind`.
        min_port : int
            The minimum port in the range of ports to try.
        max_port : int
            The maximum port in the range of ports to try.
        max_tries : int
            The number of attempt to bind.

        Returns
        -------
        port : int
            The port the socket was bound to.
        """
        for i in range(max_tries):
            try:
                port = random.randrange(min_port, max_port)
                self.bind('%s:%s' % (addr, port))
            except ZMQError:
                pass
            else:
                return port
        raise ZMQError("Could not bind socket to random port.")

    def connect(self, addr):
        """Connect to a remote 0MQ socket.

        Parameters
        ----------
        addr : str
            The address string. This has the form 'protocol://interface:port',
            for example 'tcp://127.0.0.1:5555'. Protocols supported are
            tcp, upd, pgm, iproc and ipc.
        """
        cdef int rc

        self._check_closed()

        if not isinstance(addr, str):
            raise TypeError('expected str, got: %r' % addr)
        with nogil:
            rc = zmq_connect(self.handle, addr)
        if rc != 0:
            raise ZMQError()

    def send(self, object o, int flags=0, bool copy=False):
        """ Send a message object or string
        """
        if type(o) == Message:
            self.send_msg(o)
        elif copy:
            self.send_copy(o, flags)
        else:
            msg = Message(o)
            self.send_msg(msg, flags)

    def send_copy(self, msg, int flags=0):
        """Send a message.

        This queues the message to be sent by the IO thread at a later time.

        Parameters
        ----------
        flags : int
            Any supported flag: NOBLOCK, SNDMORE.

        Returns
        -------
        result : bool
            True if message was send, False if message was not sent (EAGAIN).
        """
        cdef int rc, rc2
        cdef zmq_msg_t data
        cdef char *msg_c
        cdef Py_ssize_t msg_c_len

        self._check_closed()

        if not isinstance(msg, str):
            raise TypeError('expected str, got: %r' % msg)

        # If zmq_msg_init_* fails do we need to call zmq_msg_close?

        PyString_AsStringAndSize(msg, &msg_c, &msg_c_len)
        # Copy the msg before sending. This avoids any complications with
        # the GIL, etc.
        rc = zmq_msg_init_size(&data, msg_c_len)
        memcpy(zmq_msg_data(&data), msg_c, zmq_msg_size(&data))

        if rc != 0:
            raise ZMQError()

        with nogil:
            rc = zmq_send(self.handle, &data, flags)
        rc2 = zmq_msg_close(&data)

        # Shouldn't the error handling for zmq_msg_close come after that
        # of zmq_send?
        if rc2 != 0:
            raise ZMQError()

        if rc != 0:
            raise ZMQError()

        return True


    def send_msg(self, Message msg, int flags=0):
        """Send a message object

        This queues the message to be sent by the IO thread at a later time.

        Parameters
        ----------
        flags : int
            Any supported flag: NOBLOCK, SNDMORE.

        Returns
        -------
        result : bool
            True if message was send, raises error otherwise.
        """
        cdef int rc
        cdef zmq_msg_t data = msg.zmq_msg

        self._check_closed()

        if not msg.contains_data:
            raise ZMQError("Message does not contain any data")

        with nogil:
            rc = zmq_send(self.handle, &data, flags)

        if rc != 0:
            raise ZMQError(zmq_errno())
        msg.contains_data = False
        return True

    def recv(self, int flags=0):
        """Receive a message.

        Parameters
        ----------
        flags : int
            Any supported flag: NOBLOCK. If NOBLOCK is set, this method
            will return None if a message is not ready. If NOBLOCK is not
            set, then this method will block until a message arrives.

        Returns
        -------
        msg : str
            The returned message
        """
        cdef int rc
        cdef Message message
        cdef zmq_msg_t zmq_msg
        message = Message()

        self._check_closed()


        with nogil:
            rc = zmq_recv(self.handle, &message.zmq_msg, flags)

        if rc != 0:
            raise ZMQError(zmq_errno())
        message.contains_data = True
        return message

    def send_multipart(self, msg_parts, int flags=0):
        """Send a sequence of messages as a multipart message.

        Parameters
        ----------
        msg_parts : iterable
            A sequence of messages to send as a multipart message.
        flags : int
            Any supported flag: NOBLOCK, SNDMORE.
        """
        for msg in msg_parts[:-1]:
            self.send(msg, SNDMORE|flags)
        # Send the last part without the SNDMORE flag.
        self.send(msg_parts[-1], flags)

    def recv_multipart(self, int flags=0):
        """Receive a multipart message as a list of messages.

        Parameters
        ----------
        flags : int
            Any supported flag: NOBLOCK. If NOBLOCK is set, this method
            will return None if a message is not ready. If NOBLOCK is not
            set, then this method will block until a message arrives.

        Returns
        -------
        msg_parts : list
            A list of messages in the multipart message.
        """
        parts = []
        while True:
            part = self.recv(flags)
            parts.append(part)
            if self.rcvmore():
                continue
            else:
                break
        return parts

    def rcvmore(self):
        """Are there more parts to a multipart message."""
        more = self.getsockopt(RCVMORE)
        return bool(more)

    def send_pyobj(self, obj, flags=0, protocol=-1, ident=None):
        """Send a Python object as a message using pickle to serialize.

        Parameters
        ----------
        obj : Python object
            The Python object to send.
        flags : int
            Any valid send flag.
        protocol : int
            The pickle protocol number to use. Default of -1 will select
            the highest supported number. Use 0 for multiple platform
            support.
        ident : str
            The identity of the remote endpoint, with the length prefix
            included. This is prefixed to the message.
        """
        msg = pickle.dumps(obj, protocol)
        if ident is not None:
            msg = join_ident(ident, msg)
        return self.send(msg, flags)

    def recv_pyobj(self, flags=0, ident=False):
        """Receive a Python object as a message using pickle to serialize.

        Parameters
        ----------
        flags : int
            Any valid recv flag.

        Returns
        -------
        obj : Python object
            The Python object that arrives as a message.
        ident : bool
            If True, split off the identity and return (identity, obj).
        """
        s = self.recv(flags)
        if s is not None:
            if ident:
                ident, s = split_ident(s)
                return (ident, pickle.loads(s))
            else:
                return pickle.loads(s)

    def send_json(self, obj, flags=0, ident=None):
        """Send a Python object as a message using json to serialize.

        Parameters
        ----------
        obj : Python object
            The Python object to send.
        flags : int
            Any valid send flag.
        ident : str
            The identity of the remote endpoint, with the length prefix
            included. This is prefixed to the message.
        """
        if json is None:
            raise ImportError('json or simplejson library is required.')
        else:
            msg = json.dumps(obj, separators=(',',':'))
            if ident is not None:
                msg = join_ident(ident, msg)
            return self.send(msg, flags)

    def recv_json(self, flags=0, ident=False):
        """Receive a Python object as a message using json to serialize.

        Parameters
        ----------
        flags : int
            Any valid recv flag.
        ident : bool
            If True, split off the identity and return (identity, obj).

        Returns
        -------
        obj : Python object
            The Python object that arrives as a message.
        """
        if json is None:
            raise ImportError('json or simplejson library is required.')
        else:
            msg = self.recv(flags)
            if msg is not None:
                if ident:
                    ident, msg_buf = split_ident(msg)
                    return (ident, json.loads(str(msg_buf)))
                else:
                    return json.loads(msg)


def split_ident(msg):
    """Split a message into (identity, msg)."""
    # '\x11\x00\xbf\x1c\x9d\xd26\xb7J\xc6\x89\x9cb\x9f\xa8\xc98Yleft 15'
    ident_offset = struct.unpack('B', msg[0])[0] + 1
    ident_str = msg[:ident_offset]
    msg_buf = buffer(msg, ident_offset)
    return (ident_str, msg_buf)


def join_ident(ident, msg):
    """Prefix an identity to a message."""
    if not isinstance(msg, str):
        msg = str(msg)
    return ident + msg


def _poll(sockets, long timeout=-1):
    """Poll a set of 0MQ sockets, native file descs. or sockets.

    Parameters
    ----------
    sockets : list of tuples of (socket, flags)
        Each element of this list is a two-tuple containing a socket
        and a flags. The socket may be a 0MQ socket or any object with
        a :meth:`fileno` method. The flags can be zmq.POLLIN (for detecting
        for incoming messages), zmq.POLLOUT (for detecting that send is OK)
        or zmq.POLLIN|zmq.POLLOUT for detecting both.
    timeout : int
        The number of microseconds to poll for. Negative means no timeout.
    """
    cdef int rc, i
    cdef zmq_pollitem_t *pollitems = NULL
    cdef int nsockets = len(sockets)
    cdef Socket current_socket
    pollitems_o = allocate(nsockets*sizeof(zmq_pollitem_t),<void**>&pollitems)

    for i in range(nsockets):
        s = sockets[i][0]
        events = sockets[i][1]
        if isinstance(s, Socket):
            current_socket = s
            pollitems[i].socket = current_socket.handle
            pollitems[i].events = events
            pollitems[i].revents = 0
        elif isinstance(s, int):
            pollitems[i].socket = NULL
            pollitems[i].fd = s
            pollitems[i].events = events
            pollitems[i].revents = 0
        elif hasattr(s, 'fileno'):
            try:
                fileno = int(s.fileno())
            except:
                raise ValueError('fileno() must return an valid integer fd')
            else:
                pollitems[i].socket = NULL
                pollitems[i].fd = fileno
                pollitems[i].events = events
                pollitems[i].revents = 0
        else:
            raise TypeError(
                "Socket must be a 0MQ socket, an integer fd or have "
                "a fileno() method: %r" % s
            )

    # int zmq_poll (zmq_pollitem_t *items, int nitems, long timeout)
    with nogil:
        rc = zmq_poll(pollitems, nsockets, timeout)
    if rc == -1:
        raise ZMQError()
    
    results = []
    for i in range(nsockets):
        s = sockets[i][0]
        # Return the fd for sockets, for compat. with select.poll.
        if hasattr(s, 'fileno'):
            s = s.fileno()
        revents = pollitems[i].revents
        # Only return sockets with non-zero status for compat. with select.poll.
        if revents > 0:
            results.append((s, revents))

    return results


class Poller(object):
    """An stateful poll interface that mirrors Python's built-in poll."""

    def __init__(self):
        self.sockets = {}

    def register(self, socket, flags=POLLIN|POLLOUT):
        """Register a 0MQ socket or native fd for I/O monitoring.

        Parameters
        ----------
        socket : zmq.Socket or native socket
            A zmq.Socket or any Python object having a :meth:`fileno` 
            method that returns a valid file descriptor.
        flags : int
            The events to watch for.  Can be POLLIN, POLLOUT or POLLIN|POLLOUT.
        """
        self.sockets[socket] = flags

    def modify(self, socket, flags=POLLIN|POLLOUT):
        """Modify the flags for an already registered 0MQ socket or native fd."""
        self.register(socket, flags)

    def unregister(self, socket):
        """Remove a 0MQ socket or native fd for I/O monitoring.

        Parameters
        ----------
        socket : Socket
            The socket instance to stop polling.
        """
        del self.sockets[socket]

    def poll(self, timeout=None):
        """Poll the registered 0MQ or native fds for I/O.

        Parameters
        ----------
        timeout : int
            The timeout in microseconds. If None, no timeout (infinite).
        """
        if timeout is None:
            timeout = -1
        return _poll(self.sockets.items(), timeout=timeout)


def select(rlist, wlist, xlist, timeout=None):
    """Return the result of poll as a lists of sockets ready for r/w.

    This has the same interface as Python's built-in :func:`select` function.
    """
    if timeout is None:
        timeout = -1
    sockets = []
    for s in set(rlist + wlist + xlist):
        flags = 0
        if s in rlist:
            flags |= POLLIN
        if s in wlist:
            flags |= POLLOUT
        if s in xlist:
            flags |= POLLERR
        sockets.append((s, flags))
    return_sockets = _poll(sockets, timeout)
    rlist, wlist, xlist = [], [], []
    for s, flags in return_sockets:
        if flags & POLLIN:
            rlist.append(s)
        if flags & POLLOUT:
            wlist.append(s)
        if flags & POLLERR:
            xlist.append(s)
    return rlist, wlist, xlist
    


__all__ = [
    'Context',
    'Socket',
    'ZMQError',
    'NOBLOCK',
    'P2P',
    'PAIR',
    'PUB',
    'SUB',
    'REQ',
    'REP',
    'XREQ',
    'XREP',
    'UPSTREAM',
    'DOWNSTREAM',
    'HWM',
    'LWM',
    'SWAP',
    'AFFINITY',
    'IDENTITY',
    'SUBSCRIBE',
    'UNSUBSCRIBE',
    'RATE',
    'RECOVERY_IVL',
    'MCAST_LOOP',
    'SNDBUF',
    'RCVBUF',
    'SNDMORE',
    'RCVMORE',
    'POLL',
    'POLLIN',
    'POLLOUT',
    'POLLERR',
    '_poll',
    'select',
    'Poller',
    'Message',
    'split_ident',
    'join_ident',
    'EAGAIN',    # ERRORNO
    'EINVAL',
    'ENOTSUP',
    'EPROTONOSUPPORT',
    'ENOBUFS',
    'ENETDOWN',
    'EADDRINUSE',
    'EADDRNOTAVAIL',
    'ECONNREFUSED',
    'EINPROGRESS',
    'EMTHREAD',
    'EFSM',
    'ENOCOMPATPROTO',
    'ETERM',
]

