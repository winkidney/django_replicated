# coding: utf-8
from __future__ import unicode_literals

import etcd
from django import db


def get_object_name(obj):
    try:
        return obj.__name__
    except AttributeError:
        return obj.__class__.__name__


class Routers(object):
    def __getattr__(self, name):
        for r in db.router.routers:
            if hasattr(r, name):
                return getattr(r, name)
        msg = 'Not found the router with the method "%s".' % name
        raise AttributeError(msg)


routers = Routers()


class ETCD(object):
    master_path = "/etcd-root-path/jiajia-moha/election/master/id"

    def __init__(self, host, port):
        self._client = etcd.Client(
            host=host, port=port,
            allow_reconnect=True,
        )

    def get_mysql_master(self):
        master_id = self._client.get(self.master_path)
        return master_id
