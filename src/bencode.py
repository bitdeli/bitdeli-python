# The contents of this file are subject to the BitTorrent Open Source License
# Version 1.1 (the License).  You may not copy or use this file, in either
# source code or executable form, except in compliance with the License.  You
# may obtain a copy of the License at http://www.bittorrent.com/license/.
#
# Software distributed under the License is distributed on an AS IS basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied.  See the License
# for the specific language governing rights and limitations under the
# License.

# Written by Petru Paler

import zlib, json

def decode_int(x, f):
    f += 1
    newf = x.index('e', f)
    n = int(x[f:newf])
    if x[f] == '-':
        if x[f + 1] == '0':
            raise ValueError
    elif x[f] == '0' and newf != f+1:
        raise ValueError
    return (n, newf+1)

def decode_unicode(x, f):
    s, f = decode_string(x, f+1)
    return (s.decode('utf-8'), f)

def decode_json(x, f):
    s, f = decode_string(x, f+1)
    return (json.loads(s), f)

def decode_benjson(x, f):
    s, f = decode_string(x, f+1)
    return (BenJson(s), f)

def decode_string(x, f):
    colon = x.index(':', f)
    n = int(x[f:colon])
    if x[f] == '0' and colon != f+1:
        raise ValueError
    colon += 1
    return (x[colon:colon+n], colon+n)

def decode_list(x, f):
    r, f = [], f+1
    while x[f] != 'e':
        v, f = decode_func[x[f]](x, f)
        r.append(v)
    return (r, f + 1)

def decode_dict(x, f):
    r, f = {}, f+1
    while x[f] != 'e':
        k, f = decode_string(x, f)
        r[k], f = decode_func[x[f]](x, f)
    return (r, f + 1)

def decode_compressed(x, f):
    s, f = decode_string(x, f+1)
    return (bdecode(zlib.decompress(s)), f)

decode_func = {}
decode_func['l'] = decode_list
decode_func['d'] = decode_dict
decode_func['i'] = decode_int
decode_func['j'] = decode_json
decode_func['u'] = decode_unicode
decode_func['z'] = decode_compressed
decode_func['0'] = decode_string
decode_func['1'] = decode_string
decode_func['2'] = decode_string
decode_func['3'] = decode_string
decode_func['4'] = decode_string
decode_func['5'] = decode_string
decode_func['6'] = decode_string
decode_func['7'] = decode_string
decode_func['8'] = decode_string
decode_func['9'] = decode_string

def bdecode(x, benjson=False):
    try:
        decode_func['j'] = decode_benjson if benjson else decode_json
        r, l = decode_func[x[0]](x, 0)
    except (IndexError, KeyError, ValueError):
        raise Exception("not a valid bencoded string")
    if l != len(x):
        raise Exception("invalid bencoded value (data after valid prefix)")
    return r

from types import StringType, IntType, LongType, DictType,\
                  ListType, TupleType, UnicodeType


class BenCached(object):
    __slots__ = ['bencoded']
    def __init__(self, s):
        self.bencoded = s

class BenCompressed(object):
    __slots__ = ['data']
    def __init__(self, s):
        self.data = zlib.compress(bencode(s))

class BenJson(object):
    __slots__ = ['data']
    def __init__(self, s):
        self.data = s

def encode_bencached(x, r):
    r.append(x.bencoded)

def encode_bencompressed(x, r):
    r.extend(('z', str(len(x.data)), ':', x.data))

def encode_benjson(x, r):
    r.extend(('j', str(len(x.data)), ':', x.data))

def encode_int(x, r):
    r.extend(('i', str(x), 'e'))

def encode_bool(x, r):
    if x:
        encode_int(1, r)
    else:
        encode_int(0, r)

def encode_unicode(x, r):
    x = x.encode('utf-8')
    r.extend(('u', str(len(x)), ':', x))

def encode_string(x, r):
    r.extend((str(len(x)), ':', x))

def encode_list(x, r):
    r.append('l')
    for i in x:
        encode_func[type(i)](i, r)
    r.append('e')

def encode_dict(x,r):
    r.append('d')
    ilist = x.items()
    ilist.sort()
    for k, v in ilist:
        r.extend((str(len(k)), ':', k))
        encode_func[type(v)](v, r)
    r.append('e')

encode_func = {}
encode_func[BenCached] = encode_bencached
encode_func[BenCompressed] = encode_bencompressed
encode_func[BenJson] = encode_benjson
encode_func[IntType] = encode_int
encode_func[LongType] = encode_int
encode_func[StringType] = encode_string
encode_func[ListType] = encode_list
encode_func[TupleType] = encode_list
encode_func[DictType] = encode_dict
encode_func[UnicodeType] = encode_unicode

try:
    from types import BooleanType
    encode_func[BooleanType] = encode_bool
except ImportError:
    pass

def bencode(x):
    r = []
    encode_func[type(x)](x, r)
    return ''.join(r)
