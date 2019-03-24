#!/usr/bin/env python3

import conf
from kframe import Parent
from kframe.plugins.sql import SQL
from miner import Miner


def main():
    p = Parent(name='Mind Miner')
    p.add_module(key='conf', target=conf)
    p.add_plugin(key='sql', target=SQL, autostart=True, kwargs=conf.SQL)
    p.add_plugin(key='miner', target=Miner, autostart=True, dependes=['sql', 'conf'])
    p.init()
    p.start()
    return p


if __name__ == '__main__':
    p = main()
