#!/usr/bin/env python
# Copyright (C) 2010 Benjamin Pollack.  All rights reserved.

import datetime
import getopt
import os
import re
import pickle
import select
import socket
import sys
import threading

class RedisError(object):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return '<RedisError(%s)>' % self.message

class RedisConstant(object):
    def __init__(self, type):
        self.type = type

    def __repr__(self):
        return '<RedisConstant(%s)>' % self.type

EMPTY_SCALAR = RedisConstant('EmptyScalar')
EMPTY_LIST = RedisConstant('EmptyList')
BAD_VALUE = RedisError('Operation against a key holding the wrong kind of value')


class RedisClient(object):
    def __init__(self, socket):
        self.socket = socket
        self.wfile = socket.makefile('wb')
        self.rfile = socket.makefile('rb')
        self.db = None
        self.table = None

class MiniRedis(threading.Thread):
    def __init__(self, host='127.0.0.1', port=6379, logging=False, db_file=None):
        super(MiniRedis, self).__init__()
        self.host = host
        self.port = port
        self.logging = logging
        self.halt = True
        self.tables = {}
        self.clients = {}
        self.db_file = db_file

        self.load()

    def log(self, client, s):
        if self.logging:
            who = '%s:%s' % client.socket.getpeername() if client else 'SERVER'
            sys.stdout.write('%s - %s: %s\n' % (datetime.datetime.now(), who, s))
            sys.stdout.flush()

    def select(self, client, db):
        if db not in self.tables:
            self.tables[db] = {}
        client.db = db
        client.table = self.tables[db]

    def save(self):
        if self.db_file:
            with open(self.db_file, 'wb') as f:
                pickle.dump(self.tables, f, pickle.HIGHEST_PROTOCOL)
                self.log(None, 'saved database to "%s"' % self.db_file)

    def load(self):
        if self.db_file and os.path.lexists(self.db_file):
            with open(self.db_file, 'rb') as f:
                self.tables = pickle.load(f)
                self.log(None, 'loaded database from file "%s"' % self.db_file)

    def handle_set(self, client, key, data):
        client.table[key] = data
        self.log(client, 'SET %s -> %s' % (key, data))
        return True

    def handle_get(self, client, key):
        data = client.table.get(key, None)
        if isinstance(data, list):
            return BAD_VALUE
        if data != None:
            data = str(data)
        else:
            data = EMPTY_SCALAR
        self.log(client, 'GET %s -> %s' % (key, data))
        return data

    def handle_del(self, client, key):
        self.log(client, 'DEL %s' % key)
        if key not in client.table:
            return 0
        del client.table[key]
        return 1

    def handle_lpush(self, client, key, data):
        if key not in client.table:
            client.table[key] = []
        elif not isinstance(client.table[key], list):
            return BAD_VALUE
        client.table[key].insert(0, data)
        self.log(client, 'LPUSH %s %s' % (key, data))
        return True

    def handle_rpop(self, client, key):
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], list):
            return BAD_VALUE
        if len(client.table[key]) > 0:
            data = client.table[key].pop()
        else:
            data = EMPTY_SCALAR
        self.log(client, 'LPOP %s -> %s' % (key, data))
        return data

    def handle_lrange(self, client, key, low, high):
        low, high = int(low), int(high)
        if low == 0 and high == -1:
            high = None
        if key not in client.table:
            return EMPTY_LIST
        if not isinstance(client.table[key], list):
            return BAD_VALUE
        self.log(client, 'LRANGE %s %s %s -> %s' % (key, low, high, client.table[key][low:high]))
        return client.table[key][low:high]


    def handle_keys(self, client, pattern):
        r = re.compile(pattern.replace('*', '.*'))
        self.log(client, 'KEYS %s' % pattern)
        return ' '.join(k for k in client.table.keys() if r.search(k))

    def handle_incr(self, client, key):
        return self.handle_incrby(client, key, 1)

    def handle_incrby(self, client, key, by):
        try:
            client.table[key] = int(client.table[key])
            client.table[key] += int(by)
        except (KeyError, TypeError, ValueError):
            client.table[key] = 1
        self.log(client, 'INCRBY %s %s -> %s' % (key, by, client.table[key]))
        return client.table[key]

    def handle_flushdb(self, client):
        self.log(client, 'FLUSHDB')
        client.table.clear()
        return True

    def handle_flushall(self, client):
        self.log(client, 'FLUSHALL')
        for table in self.tables.itervalues():
            table.clear()
        return True

    def handle_select(self, client, db):
        db = int(db)
        self.select(client, db)
        self.log(client, 'SELECT %s' % db)
        return True

    def handle_quit(self, client):
        self.log(client, 'QUIT')
        client.socket.shutdown(socket.SHUT_RDWR)
        client.socket.close()
        del self.clients[client.socket]
        return False

    def handle_shutdown(self, client):
        self.log(client, 'SHUTDOWN')
        self.halt = True
        return True

    def handle_save(self, client):
        self.save()
        self.log(client, 'SAVE')
        return True

    def handle(self, client):
        line = client.rfile.readline()
        if not line:
            self.log(client, 'client disconnected')
            del self.clients[client.socket]
            client.socket.close()
            return
        items = int(line[1:].strip())
        args = []
        for x in xrange(0, items):
            length = int(client.rfile.readline().strip()[1:])
            args.append(client.rfile.read(length))
            client.rfile.read(2) # throw out newline
        command = args[0].lower()
        self.dump(getattr(self, 'handle_' + command)(client, *args[1:]))

    def dump(self, client, o):
        nl = '\r\n'
        if isinstance(o, bool):
            if o:
                client.wfile.write('+OK\r\n')
            # Show nothing for a false return; that means be quiet
        elif o == EMPTY_SCALAR:
            client.wfile.write('$-1\r\n')
        elif o == EMPTY_LIST:
            client.wfile.write('*-1\r\n')
        elif isinstance(o, int):
            client.wfile.write(':' + str(o) + nl)
        elif isinstance(o, str):
            client.wfile.write('$' + str(len(o)) + nl)
            client.wfile.write(o + nl)
        elif isinstance(o, list):
            client.wfile.write('*' + str(len(o)) + nl)
            for val in o:
                self.dump(client, str(val))
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
                        self.handle_quit(client)
        for client_socket in self.clients.iterkeys():
            client_socket.close()
        self.clients.clear()
        server.close()
        server = None

def main(args):
    host, port, logging, db_file = '127.0.0.1', 6379, True, None
    opts, args = getopt.getopt(args, 'h:p:d:l')
    for o, a in opts:
        if o == '-h':
            host = a
        elif o == '-p':
            port = int(a)
        elif o == '-l':
            logging = True
        elif o == '-d':
            db_file = os.path.abspath(a)
    print 'Launching MiniRedis on %s:%s' % (host, port)
    m = MiniRedis(host=host, port=port, logging=logging, db_file=db_file)
    m.start()
    m.join()
    print 'Stopped'

if __name__ == '__main__':
    main(sys.argv[1:])
