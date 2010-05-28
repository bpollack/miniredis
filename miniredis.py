#!/usr/bin/env python

import getopt
import re
import select
import socket
import sys
import threading

class RedisError(object):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return '<RedisError(%s)>' % self.message

class RedisBadValueError(RedisError):
    def __init__(self):
        super(RedisBadValueError, self).__init__('Operation against a key holding the wrong kind of value')

class RedisConstant(object):
    def __init__(self, type):
        self.type = type

    def __repr__(self):
        return '<RedisConstant(%s)>' % self.type

EMPTY_SCALAR = RedisConstant('EmptyScalar')
EMPTY_ARRAY = RedisConstant('EmptyArray')


class RedisClient(object):
    def __init__(self, socket):
        self.socket = socket
        self.wfile = socket.makefile('wb')
        self.rfile = socket.makefile('rb')
        self.db = None
        self.table = None

class MiniRedis(threading.Thread):
    def __init__(self, host='127.0.0.1', port=56784, logging=False):
        super(MiniRedis, self).__init__()
        self.host = host
        self.port = port
        self.logging = logging
        self.halt = True
        self.tables = {}
        self.clients = {}

        self.wrappers = {'set': self.handle_set,
                         'get': self.handle_get,
                         'incr': self.handle_incr,
                         'incrby': self.handle_incrby,
                         'del': self.handle_del,
                         'keys': self.handle_keys,
                         'rpop': self.handle_rpop,
                         'lpush': self.handle_lpush,
                         'flushdb': self.handle_flushdb,
                         'select': self.handle_select,
                         'quit': self.handle_quit,
                         'shutdown': self.handle_shutdown}

        self.dispatch = {'set': self.set,
                         'get': self.get,
                         'incr': self.incr,
                         'incrby': self.incr,
                         'del': self.delete,
                         'keys': self.keys,
                         'rpop': self.rpop,
                         'lpush': self.lpush,
                         'flushdb': self.flushdb,
                         'select': self.select,
                         'quit': self.quit,
                         'shutdown': self.shutdown}

    def log(self, client, s):
        if self.logging:
            print '%s:%s: %s' % (client.socket.getpeername() + (s,))

    def set(self, client, key, data):
        client.table[key] = data
        self.log(client, 'SET %s -> %s' % (key, data))
        return True

    def get(self, client, key):
        data = client.table.get(key, None)
        if isinstance(data, list):
            return RedisBadValueError()
        if data != None:
            data = str(data)
        else:
            data = EMPTY_SCALAR
        self.log(client, 'GET %s -> %s' % (key, data))
        return data

    def delete(self, client, key):
        self.log(client, 'DEL %s' % key)
        if key not in client.table:
            return 0
        del client.table[key]
        return 1

    def lpush(self, client, key, data):
        if key not in client.table:
            client.table[key] = []
        elif not isinstance(client.table[key], list):
            return RedisBadValueError()
        client.table[key].insert(0, data)
        self.log(client, 'LPUSH %s %s' % (key, data))
        return True

    def rpop(self, client, key):
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], list):
            return RedisBadValueError()
        if len(client.table[key]) > 0:
            data = client.table[key].pop()
        else:
            data = EMPTY_SCALAR
        self.log(client, 'LPOP %s -> %s' % (key, data))
        return data

    def keys(self, client, pattern):
        r = re.compile(pattern.replace('*', '.*'))
        self.log(client, 'KEYS %s' % pattern)
        return ' '.join(k for k in client.table.keys() if r.search(k))

    def incr(self, client, key, by=1):
        try:
            client.table[key] = int(client.table[key])
            client.table[key] += int(by)
        except (KeyError, TypeError, ValueError):
            client.table[key] = 1
        self.log(client, 'INCRBY %s %s -> %s' % (key, by, client.table[key]))
        return client.table[key]

    def flushdb(self, client):
        self.log(client, 'FLUSHDB')
        client.table.clear()
        return True

    def select(self, client, db):
        db = int(db)
        if db not in self.tables:
            self.tables[db] = {}
        client.db = db
        client.table = self.tables[db]
        self.log(client, 'SELECT %s' % db)
        return True

    def quit(self, client):
        self.log(client, 'QUIT')
        client.socket.shutdown(socket.SHUT_RDWR)
        client.socket.close()
        del self.clients[client.socket]
        return False

    def shutdown(self, client):
        self.log(client, 'SHUTDOWN')
        self.halt = True
        return True

    def handle_set(self, client, line):
        key, length = line.split()
        data = client.rfile.read(int(length))
        client.rfile.read(2) # throw out newline
        return self.set(key, client, data)

    def handle_get(self, client, line):
        key = line.strip()
        return self.get(client, key)

    def handle_del(self, client, line):
        key = line.strip()
        return self.delete(client, key)

    def handle_lpush(self, client, line):
        key, length = line.split()
        data = client.rfile.read(int(length))
        client.rfile.read(2) # throw out newline
        return self.lpush(client, key, data)

    def handle_rpop(self, client, line):
        key = line.strip()
        return self.rpop(client, key)

    def handle_keys(self, client, line):
        pattern = line.strip()
        return self.keys(client, pattern)

    def handle_incr(self, client, line):
        key = line.strip()
        return self.incr(client, key)

    def handle_incrby(self, client, line):
        key, by = line.split()
        return self.incr(client, key, by)

    def handle_flushdb(self, client, line):
        return self.flushdb(client)

    def handle_select(self, client, line):
        db = line.strip()
        return self.select(client, db)

    def handle_quit(self, client, line):
        return self.quit(client)

    def handle_shutdown(self, client, line):
        return self.shutdown(client)

    def handle_normal(self, client, line):
        command, _, rest = line.partition(' ')
        return self.wrappers[command.strip().lower()](client, rest)

    def handle_multibulk(self, client, line):
        items = int(line[1:].strip())
        args = []
        for x in xrange(0, items):
            length = int(client.rfile.readline().strip()[1:])
            args.append(client.rfile.read(length))
            client.rfile.read(2) # throw out newline
        command = args[0].lower()
        return self.dispatch[command](client, *args[1:])

    def handle(self, client):
        line = client.rfile.readline()
        if not line:
            self.log(client, 'client disconnected')
            del self.clients[client.socket]
            client.socket.close()
            return
        if line[0] == '*':
            o = self.handle_multibulk(client, line)
        else:
            o = self.handle_normal(client, line)
        self.dump(client, o)

    def dump(self, client, o):
        nl = '\r\n'
        if isinstance(o, bool):
            if o:
                client.wfile.write('+OK\r\n')
            # Show nothing for a false return; that means be quiet
        elif o == EMPTY_SCALAR:
            client.wfile.write('$-1\r\n')
        elif o == EMPTY_ARRAY:
            client.wfile.write('*-1\r\n')
        elif isinstance(o, int):
            client.wfile.write(':' + str(o) + nl)
        elif isinstance(o, str):
            client.wfile.write('$' + str(len(o)) + nl)
            client.wfile.write(o + nl)
        elif isinstance(o, list):
            client.wfile.write('*' + len(o))
            for val in o:
                self.dump(client, val)
        elif isinstance(o, RedisError):
            client.wfile.write('-ERR %s\r\n' % o.message)
        else:
            client.wfile.write('return type not yet implemented\r\n')
        client.wfile.flush()

    def stop(self):
        self.halt = True
        self.join()

    def run(self):
        self.halt = False
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        while not self.halt:
            readable, _, _ = select.select([server] + self.clients.keys(), [], [], 1.0)
            for sock in readable:
                if sock == server:
                    (client_socket, address) = server.accept()
                    client = RedisClient(client_socket)
                    self.clients[client_socket] = client
                    self.log(client, 'client connected')
                    self.select(client, 0)
                else:
                    try:
                        self.handle(self.clients[sock])
                    except Exception, e:
                        self.log(client, 'exception: %s' % e)
                        self.quit(client)
        for client_socket in self.clients.iterkeys():
            client_socket.close()
        self.clients.clear()
        server.close()
        server = None

def main(args):
    host, port, logging = '127.0.0.1', 56784, True
    opts, args = getopt.getopt(args, 'h:p:l')
    for o, a in opts:
        if o == '-h':
            host = a
        elif o == '-p':
            port = int(a)
        elif o == '-l':
            logging = True
    print 'Launching MiniRedis on %s:%s' % (host, port)
    m = MiniRedis(host=host, port=port, logging=logging)
    m.start()
    m.join()
    print 'Stopped'

if __name__ == '__main__':
    main(sys.argv[1:])
