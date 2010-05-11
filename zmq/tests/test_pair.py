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

import zmq
from zmq.tests import BaseZMQTestCase

#-----------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------

class TestP2p(BaseZMQTestCase):

    def test_basic(self):
        msg1 = zmq.Message("blabla")
        msg2 = zmq.Message("blabla")
        str1 = str(msg1)
        str2 = str(msg2)
        self.assertEquals(str1, str2)

        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        import time
        time.sleep(1)
        msg = "message1"
        #msg1 = str_to_msg(s)
        #msg1 = zmq.Message(s)
        time.sleep(1)
        msg2 = self.ping_pong(s1, s2, msg)
        self.assertEquals(msg, str(msg2))

    def test_multiple(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        for i in range(100):
            #msg = zmq.Message(i*'X')
            s1.send(i*'X')

        for i in range(100):
            #msg = zmq.Message(i*'X')
            s2.send(i*'X')

        for i in range(100):
            msg = s1.recv()
            self.assertEquals(str(msg), i*'X')

        for i in range(100):
            msg = s2.recv()
            self.assertEquals(str(msg), i*'X')


if __name__ == '__main__':
    import unittest
    unittest.main()
