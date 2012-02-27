#
# Bitdeli Python worker
#
# https://bitdeli.com
#
# Copyright (c) 2012 Bitdeli Inc
#
# See LICENSE for the full MIT license

import os, sys, zlib, bencode, cStringIO, atexit, json
from itertools import izip
from struct import unpack

SYS_FIELDS = ['object', 'event_id', 'timestamp', 'group_key', 'sort_key']
MAX_MESSAGE_LENGTH = 1024 * 1024
nonce = ''

class LogWriter(object):
    def write(self, string):
        string = string.strip()
        if string:
            log(string)

class OutputBuffer(object):
    def __init__(self):
        self.buffer = cStringIO.StringIO()
        self.buffer.write('l')
        self.size = 0

    def add(self, e):
        e = bencode.bencode(e)
        if self.size + len(e) < MAX_MESSAGE_LENGTH:
            self.size += len(e)
            self.buffer.write(e)
        else:
            self.flush()
            self.add(e)

    def flush(self):
        if self.size > 0:
            self.buffer.write('e')
            communicate('out', self.buffer.getvalue())
            self.buffer.truncate(0)
            self.buffer.write('l')
            self.size = 0

class Event(dict):
    def __init__(self, input):
        super(Event, self).__init__()
        if isinstance(input, list):
            self._init_sys(input)
        else:
            self._init_user(input)

    def _init_sys(self, input):
        self.update(input[0])
        for (field, value) in izip(SYS_FIELDS[1:], input[1:]):
            setattr(self, field, value)

    def _init_user(self, input):
        for field in SYS_FIELDS[1:]:
            if field in input:
                setattr(self, field, input)
        if 'object' in input:
            self.update(input['object'])
        else:
            self.update(input)

def read_int():
    buf = ''
    for i in range(11):
        buf += sys.stdin.read(1)
        if buf[-1] == ' ':
            return int(buf)
    raise Exception("System error: Invalid length (%s)" % buf)

def recv():
    global nonce
    nonce = sys.stdin.read(5)[:4]
    return sys.stdin.read(read_int())

def communicate(head, body='', benjson=False):
    sys.__stdout__.write('%s %s %d %s\n' % (nonce, head, len(body), body))
    reply = recv()
    if reply:
        return bencode.bdecode(reply, benjson)
    else:
        return ''

def events():
    while True:
        input = communicate('next')
        if len(input) > 0:
            yield Event(input)
        else:
            break

def output(item):
    if isinstance(item, dict):
        enc = json.dumps(item)
        if len(enc) < MAX_MESSAGE_LENGTH:
            output_buffer.add(bencode.BenJson(enc))
        else:
            raise Exception("Item too large! (%d > %d bytes)" %
                            (len(benc), MAX_MESSAGE_LENGTH))
    else:
        raise Exception("Output accepts dictionaries only")

def done():
    return communicate('done')

def ping():
    return communicate('ping')

def log(msg):
    communicate('log', bencode.bencode(unicode(msg).encode('utf-8')))

def flush_before_traceback(type, value, traceback):
    output_buffer.flush()
    sys.__excepthook__(type, value, traceback)

def init():
    global output_buffer
    sys.stdout = LogWriter()
    output_buffer = OutputBuffer()
    sys.excepthook = flush_before_traceback
    atexit.register(output_buffer.flush)
    if 'TESTING' not in os.environ:
        ret = recv()
        if ret != '2:ok':
            raise Exception("System error: Invalid initial reply (%s)" % ret)
        ping()

init()
