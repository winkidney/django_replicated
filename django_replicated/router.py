# coding: utf-8
from __future__ import unicode_literals

import logging
import random
from threading import local

from django_replicated.utils import ETCD

log = logging.getLogger(__name__)


class ReplicationRouter(object):

    def __init__(self):
        from django.db import DEFAULT_DB_ALIAS
        from django.conf import settings

        self._context = local()

        self.DEFAULT_DB_ALIAS = DEFAULT_DB_ALIAS
        self.DOWNTIME = settings.REPLICATED_DATABASE_DOWNTIME
        self.SLAVES = settings.REPLICATED_DATABASE_SLAVES or [DEFAULT_DB_ALIAS]
        self.CHECK_STATE_ON_WRITE = settings.REPLICATED_CHECK_STATE_ON_WRITE

        self.all_allowed_aliases = [self.DEFAULT_DB_ALIAS] + self.SLAVES

        self._etcd = ETCD(settings.ETCD_REPLICATED['host'], settings.ETCD_REPLICATED['port'])

    def reset(self):
        self._context.state_stack = []
        self._context.chosen = {}
        self._context.state_change_enabled = True
        self._context.inited = True

    @property
    def context(self):
        if not getattr(self._context, 'inited', False):
            self.reset()
        return self._context

    def init(self, state):
        self.reset()
        self.use_state(state)

    def is_alive(self, db_name):
        from .dbchecker import db_is_alive

        return db_is_alive(db_name, self.DOWNTIME)

    def set_state_change(self, enabled):
        self.context.state_change_enabled = enabled

    def state(self):
        '''
        Current state of routing: 'master' or 'slave'.
        '''
        if self.context.state_stack:
            return self.context.state_stack[-1]
        else:
            return 'master'

    def use_state(self, state):
        '''
        Switches router into a new state. Requires a paired call
        to 'revert' for reverting to previous state.
        '''
        if not self.context.state_change_enabled:
            state = self.state()
        self.context.state_stack.append(state)
        return self

    def revert(self):
        '''
        Reverts wrapper state to a previous value after calling
        'use_state'.
        '''
        self.context.state_stack.pop()

    def get_master_name(self):
        from django.conf import settings
        master_host = self._etcd.get_mysql_master()

        for name, database in settings.DATABASES.items():
            if database['HOST'] == master_host:
                return name
        raise ValueError("Given node not found")

    def db_for_write(self, *args, **kwargs):
        if self.CHECK_STATE_ON_WRITE and self.state() != 'master':
            try:
                master_name = self.get_master_name()
                self.use_state("master")
            except ValueError:
                raise RuntimeError('Trying to access master database in slave state')
        else:
            master_name = self.DEFAULT_DB_ALIAS
        self.context.chosen['master'] = master_name
        log.debug('db_for_write: %s', master_name)
        return master_name

    def db_for_read(self, *args, **kwargs):
        if self.state() == 'master':
            return self.db_for_write(*args, **kwargs)

        if self.state() in self.context.chosen:
            return self.context.chosen[self.state()]

        slaves = self.SLAVES[:]
        random.shuffle(slaves)

        for slave in slaves:
            if self.is_alive(slave):
                chosen = slave
                break
        else:
            chosen = self.DEFAULT_DB_ALIAS

        self.context.chosen[self.state()] = chosen

        log.debug('db_for_read: %s', chosen)
        return chosen

    def allow_relation(self, obj1, obj2, **hints):
        for db in (obj1._state.db, obj2._state.db):
            if db is not None and db not in self.all_allowed_aliases:
                return False

        return True
