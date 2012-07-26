#!/usr/bin/env python

# This is the MIT License
# http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2007,2008 Nick Galbreath
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

#
# Version 1.0 - 21-Apr-2007
#   initial
# Version 2.0 - 16-Nov-2008
#   made class Gmetric thread safe
#   made gmetrix xdr writers _and readers_
#   Now this only works for gmond 2.X packets, not tested with 3.X
#
# Version 3.0 - 09-Jan-2011 Author: Vladimir Vuksan
#   Made it work with the Ganglia 3.1 data format
#
# Version 3.1 - 30-Apr-2011 Author: Adam Tygart
#   Added Spoofing support
#
# Version 3.1.1 - 26-Jul-2012 Author: Nick Satterly <nick.satterly@guardian.co.uk>
#  Added heartbeat support, non-string metrics, title and description

import sys
import optparse
from xdrlib import Packer, Unpacker
import socket

__version__ = '3.1.1'

SLOPES = {
    'zero':        0,
    'positive':    1,
    'negative':    2,
    'both':        3,
    'unspecified': 4
}

TYPES = {
    'int8':   131,
    'uint8':  131,
    'int16':  131,
    'uint16': 131,
    'int32':  131,
    'uint32': 132,
    'string': 133,
    'float':  134,
    'double': 135
}

class Gmetric:
    """
    Class to send gmetric/gmond 2.X packets

    Thread safe
    """

    protocol = ('udp', 'multicast')

    def __init__(self, host, port, protocol):
        if protocol not in self.protocol:
            raise ValueError("Protocol must be one of: " + str(self.protocol))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if protocol == 'multicast':
            self.socket.setsockopt(socket.IPPROTO_IP,
                                   socket.IP_MULTICAST_TTL, 20)
        self.hostport = (host, int(port))

    def metric_send(self, name, value, type, units, slope, tmax, dmax, group, title, description, spoof):

        if name is None or value is None or type is None or slope not in SLOPES:
            print >>sys.stderr, 'gmetric parameters invalid. exiting.'
            sys.exit(1)
        if '"' in name or '"' in value or '"' in type or '"' in units:
            print >>sys.stderr, 'one of your parameters has an invalid character \'"\'. exiting.'
            sys.exit(1)
        if type not in TYPES.keys():
            print >>sys.stderr, 'the type parameter "%s" is not a valid type. exiting.' % (type)
            sys.exit(1)
        if type != 'string':
            try:
                int(value)
            except ValueError:
                pass
            try:
                float(value)
            except ValueError:
                print >>sys.stderr, 'the value parameter "%s" does not represent a number. exiting.' % (value)
                sys.exit(1)

        (meta_msg, data_msg) = self._gmetric(name, value, type, units, slope, tmax, dmax, group, title, description, spoof)

        self.socket.sendto(meta_msg, self.hostport)
        self.socket.sendto(data_msg, self.hostport)

    def _gmetric(self, name, val, type, units, slope, tmax, dmax, group, title, description, spoof):
        """
        Arguments are in all upper-case to match XML
        """

        meta = Packer()
        HOSTNAME=socket.gethostname()
        if spoof == "":
            SPOOFENABLED=0
        else :
            SPOOFENABLED=1

        # Meta data about a metric
        packet_type = 128
        meta.pack_int(packet_type)
        if SPOOFENABLED == 1:
            meta.pack_string(spoof)
        else:
            meta.pack_string(HOSTNAME)
        meta.pack_string(name)
        meta.pack_int(SPOOFENABLED)
        meta.pack_string(type)
        meta.pack_string(name)
        meta.pack_string(units)
        meta.pack_int(SLOPES[slope]) # map slope string to int
        meta.pack_uint(int(tmax))
        meta.pack_uint(int(dmax))

        extra_data = 0
        if group != "":
            extra_data += 1
        if title != "":
            extra_data += 1
        if description != "":
            extra_data += 1

        meta.pack_int(extra_data)
        if group != "":
            meta.pack_string("GROUP")
            meta.pack_string(group)
        if title != "":
            meta.pack_string("TITLE")
            meta.pack_string(title)
        if description != "":
            meta.pack_string("DESC")
            meta.pack_string(description)

        # Actual data sent in a separate packet
        data = Packer()
        packet_type = TYPES[type]
        data.pack_int(packet_type)
        if SPOOFENABLED == 1:
            data.pack_string(spoof)
        else:
            data.pack_string(HOSTNAME)
        data.pack_string(name)
        data.pack_int(SPOOFENABLED)

        if type in ['int8','uint8','int16','uint16','int32']:
            data.pack_string("%d")
            data.pack_int(int(val))
        if type == 'uint32':
            data.pack_string("%u")
            data.pack_uint(long(val))
        if type == 'string':
            data.pack_string("%s")
            data.pack_string(str(val))
        if type == 'float':
            data.pack_string("%f")
            data.pack_float(float(val))
        if type == 'double':
            data.pack_string("%f")
            data.pack_double(float(val))  # XXX - double or float?

        return (meta.get_buffer(), data.get_buffer())

def main():
    parser = optparse.OptionParser(
                      version="%prog " + __version__,
                      description="Python version of the Ganglia Metric Client (gmetric) ")
    parser.add_option("", "--protocol", dest="protocol", default="udp",
                      help="The gmetric internet protocol, either udp or multicast, default udp")
    parser.add_option("", "--host",  dest="host",  default="127.0.0.1",
                      help="GMond aggregator hostname to send data to")
    parser.add_option("", "--port",  dest="port",  default="8649",
                      help="GMond aggregator port to send data to")
    parser.add_option("-n", "--name", dest="name",
                      help="Name of the metric")
    parser.add_option("-v", "--value", dest="value",
                      help="Value of the metric")
    parser.add_option("-t", "--type", dest="type",
                      help="Either string|int8|uint8|int16|uint16|int32|uint32|float|double")
    parser.add_option("-u", "--units", dest="units", default="",
                      help="Unit of measure for the value e.g. Kilobytes, Celcius")
    parser.add_option("-s", "--slope", dest="slope", default="both",
                      help="Either zero|positive|negative|both")
    parser.add_option("-x", "--tmax", dest="tmax", default="60",
                      help="The maximum time in seconds between gmetric calls")
    parser.add_option("-d", "--dmax", dest="dmax", default="0",
                      help="The lifetime in seconds of this metric")
    parser.add_option("-g", "--group", dest="group", default="",
                      help="Group of the metric")
    parser.add_option("-D", "--desc", dest="description", default="",
                      help="Description of the metric")
    parser.add_option("-T", "--title", dest="title", default="",
                      help="Title of the metric")
    parser.add_option("-S", "--spoof", dest="spoof", default="",
                      help="IP address and name of host/device (colon separated) we are spoofing")
    parser.add_option("-H", "--heartbeat", action="store_true", dest="heartbeat", default=False,
                      help="spoof a heartbeat message (use with spoof option)")
    (options,args) = parser.parse_args()

    g = Gmetric(options.host, options.port, options.protocol)

    if options.heartbeat and options.spoof is None:
        print "Incorrect options supplied, exiting."
        sys.exit(1)

    if not options.heartbeat and (options.name is None or options.value is None or options.type is None):
        print "Incorrect options supplied, exiting."
        sys.exit(1)

    if options.heartbeat:
        g.metric_send("heartbeat", "0", "uint32", "", "zero", 0, 0, "", "", "", options.spoof)
    else:
        g.metric_send(options.name, options.value, options.type, options.units,
            options.slope, options.tmax, options.dmax, options.group, options.title, options.description, options.spoof)

if __name__ == '__main__':
    main()
