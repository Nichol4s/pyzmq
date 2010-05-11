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

from unittest import TestCase

import zmq
from zmq.tests import BaseZMQTestCase

#-----------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------

class TestPubSub(BaseZMQTestCase):

    def test_basic(self):
        s1, s2 = self.create_bound_pair(zmq.PUB, zmq.SUB)
        s2.setsockopt(zmq.SUBSCRIBE,'')
        import time; time.sleep(0.1)
        msg = 'message'
        s1.send(msg)
        msg2 = s2.recv()
        self.assertEquals(msg, str(msg2))

    # This test is failing on Windows due a problem with errno not 
    # being thread safe. In socket_base.cpp:recv, if zmq.NOBLOCK is set
    # xrecv is called and returns an error code of 11 (EAGAIN), but when
    # the Python bindings get errno, it has been set back to 0. Our hypothesis
    # is that another thread is doing this.
    # 
    # Note: I modified the test to follow the new ERROR handler not sure
    #       if the message above still applies. 
    def test_topic(self):
        s1, s2 = self.create_bound_pair(zmq.PUB, zmq.SUB)
        s2.setsockopt(zmq.SUBSCRIBE,'x')
        import time; time.sleep(0.1)
        s1.send('message')
        self.assertRaisesErrno(zmq.EAGAIN, s2.recv, zmq.NOBLOCK)
        s1.send('xmessage')
        msg2 = s2.recv()
        self.assertEquals('xmessage', str(msg2))

if __name__ == '__main__':
    import unittest
    unittest.main()
