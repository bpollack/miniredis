#!/usr/bin/env python
# Copyright (C) 2010 Benjamin Pollack.  All rights reserved.

from __future__ import with_statement

import datetime
import errno
import getopt
import os
import re
import pickle
import select
import signal
import socket
import sys

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

class MiniRedis(object):
    def __init__(self, host='127.0.0.1', port=6379, log_file=None, db_file=None):
        super(MiniRedis, self).__init__()
        self.host = host
        self.port = port
        if log_file:
            self.log_name = log_file
            self.log_file = open(self.log_name, 'w')
        else:
            self.log_name = None
            self.log_file = sys.stdout
        self.halt = True

        self.clients = {}
        self.tables = {}
        self.db_file = db_file

        self.load()

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

    def load(self):
        if self.db_file and os.path.lexists(self.db_file):
            with open(self.db_file, 'rb') as f:
                self.tables = pickle.load(f)
                self.log(None, 'loaded database from file "%s"' % self.db_file)

    def log(self, client, s):
        try:
            who = '%s:%s' % client.socket.getpeername() if client else 'SERVER'
        except:
            who = '<CLOSED>'
        self.log_file.write('%s - %s: %s\n' % (datetime.datetime.now(), who, s))
        self.log_file.flush()

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
        self.dump(client, getattr(self, 'handle_' + command)(client, *args[1:]))

    def rotate(self):
        self.log_file.close()
        self.log_file = open(self.log_name, 'w')

    def run(self):
        self.halt = False
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        while not self.halt:
            try:
                readable, _, _ = select.select([server] + self.clients.keys(), [], [], 1.0)
            except select.error, e:
                if e.args[0] == errno.EINTR:
                    continue
                raise
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

    def save(self):
        if self.db_file:
            with open(self.db_file, 'wb') as f:
                pickle.dump(self.tables, f, pickle.HIGHEST_PROTOCOL)

    def select(self, client, db):
        if db not in self.tables:
            self.tables[db] = {}
        client.db = db
        client.table = self.tables[db]

    def stop(self):
        if not self.halt:
            self.log(None, 'STOPPING')
            self.halt = True

    # HANDLERS

    def handle_del(self, client, key):
        self.log(client, 'DEL %s' % key)
        if key not in client.table:
            return 0
        del client.table[key]
        return 1

    def handle_flushdb(self, client):
        self.log(client, 'FLUSHDB')
        client.table.clear()
        return True

    def handle_flushall(self, client):
        self.log(client, 'FLUSHALL')
        for table in self.tables.itervalues():
            table.clear()
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

    def handle_keys(self, client, pattern):
        r = re.compile(pattern.replace('*', '.*'))
        self.log(client, 'KEYS %s' % pattern)
        return ' '.join(k for k in client.table.keys() if r.search(k))

    def handle_lpush(self, client, key, data):
        if key not in client.table:
            client.table[key] = []
        elif not isinstance(client.table[key], list):
            return BAD_VALUE
        client.table[key].insert(0, data)
        self.log(client, 'LPUSH %s %s' % (key, data))
        return True

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

    def handle_quit(self, client):
        client.socket.shutdown(socket.SHUT_RDWR)
        client.socket.close()
        self.log(client, 'QUIT')
        del self.clients[client.socket]
        return False

    def handle_save(self, client):
        try:
            if not os.fork():
                self.save()
                sys.exit(0)
        except OSError:
            self.save()
        self.log(client, 'SAVE')
        return True

    def handle_select(self, client, db):
        db = int(db)
        self.select(client, db)
        self.log(client, 'SELECT %s' % db)
        return True

    def handle_set(self, client, key, data):
        client.table[key] = data
        self.log(client, 'SET %s -> %s' % (key, data))
        return True

    def handle_setnx(self, client, key, data):
        if key in client.table:
            self.log(client, 'SETNX %s -> %s FAILED' % (key, data))
            return 0
        client.table[key] = data
        self.log(client, 'SETNX %s -> %s' % (key, data))
        return 1

    def handle_shutdown(self, client):
        self.log(client, 'SHUTDOWN')
        self.halt = True
        return self.handle_quit(client)

def main(args):
    def sigterm(signum, frame):
        m.stop()
    def sighup(signum, frame):
        m.rotate()
    signal.signal(signal.SIGTERM, sigterm)
    signal.signal(signal.SIGHUP, sighup)

    host, port, log_file, db_file = '127.0.0.1', 6379, None, None
    opts, args = getopt.getopt(args, 'h:p:d:l:')
    for o, a in opts:
        if o == '-h':
            host = a
        elif o == '-p':
            port = int(a)
        elif o == '-l':
            log_file = os.path.abspath(a)
        elif o == '-d':
            db_file = os.path.abspath(a)
    m = MiniRedis(host=host, port=port, log_file=log_file, db_file=db_file)
    try:
        m.run()
    except KeyboardInterrupt:
        m.stop()
    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
