#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import json
import pymysql
import uuid
import time

from utils.config import config as __config
from utils.datetime_util import okex_contract_type
from dateutil.parser import parse
from utils.redis_util import master_redis_client
from utils.const import const


def get_monitor_params():
    monitor_params = {
        'monitor_port': {__config.redis_trade_port: {'path': 'master01/master00.conf', 'whether_send_mail': True,
                                                     'whether_restart': True,
                                                     'whether_monitor_alarm': True, 'port_introduction': 'redis服务运行状态',
                                                     'unattended': {'Monday-Friday': [], 'Weekend': []}},
                         __config.redis_master_port: {'path': 'master00/master01.conf', 'whether_send_mail': True,
                                                      'whether_restart': True,
                                                      'whether_monitor_alarm': True, 'port_introduction': 'redis服务运行状态',
                                                      'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                     'Weekend': ['0:00-23:59']}},
                         __config.redis_slave_port: {'path': '', 'whether_send_mail': True, 'whether_restart': True,
                                                     'whether_monitor_alarm': True,
                                                     'port_introduction': 'redis服务运行状态',
                                                     'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                    'Weekend': ['0:00-23:59']}},
                         __config.redis_lock_port: {'path': '', 'whether_send_mail': True, 'whether_restart': True,
                                                    'whether_monitor_alarm': True,
                                                    'port_introduction': 'redis服务运行状态',
                                                    'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                   'Weekend': ['0:00-23:59']}},
                         __config.redis_port: {'path': '', 'whether_send_mail': True, 'whether_restart': True,
                                               'whether_monitor_alarm': True,
                                               'port_introduction': 'redis服务运行状态',
                                               'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                              'Weekend': ['0:00-23:59']}},
                         __config.zmq_rep_port: {'path': 'strategy_deal_recv.py', 'whether_send_mail': True,
                                                 'whether_restart': True,
                                                 'whether_monitor_alarm': True, 'port_introduction': '交易服务运行状态',
                                                 'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                'Weekend': ['0:00-23:59']}},
                         __config.mysql_trade_port: {'path': __config.mysql_path, 'whether_send_mail': True,
                                                     'whether_restart': True,
                                                     'whether_monitor_alarm': True, 'port_introduction': 'mysql服务运行状态',
                                                     'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                    'Weekend': ['0:00-23:59']}},
                         __config.zmq_assets_port: {'path': '', 'whether_send_mail': True, 'whether_restart': True,
                                                    'whether_monitor_alarm': True, 'port_introduction': '策略资产服务运行状态',
                                                    'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                   'Weekend': ['0:00-23:59']}},
                         __config.zmq_positions_port: {'path': '', 'whether_send_mail': True, 'whether_restart': True,
                                                       'whether_monitor_alarm': True, 'port_introduction': '策略持仓服务运行状态',
                                                       'unattended': {'Monday-Friday': ['0:00-23:59'],
                                                                      'Weekend': ['0:00-23:59']}}},
        'monitor_memoryspace': {'redis_space': {
            __config.redis_trade_port: {'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                                        'whether_automatic_handle': True,
                                        'introduction': '内存监控',
                                        'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}},
            __config.redis_master_port: {'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                                         'whether_automatic_handle': True,
                                         'introduction': '内存监控',
                                         'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}},
            __config.redis_slave_port: {'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                                        'whether_automatic_handle': True,
                                        'introduction': '内存监控',
                                        'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}},
            __config.redis_lock_port: {'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                                       'whether_automatic_handle': True,
                                       'introduction': '内存监控',
                                       'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}},
            __config.redis_port: {'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                                  'whether_automatic_handle': True,
                                  'introduction': '内存监控',
                                  'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}}},
            'file_space': [
                {'directory': '/data', 'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                 'whether_automatic_handle': True, 'introduction': '磁盘监控',
                 'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}},
                {'directory': '/var', 'risk_level': 3, 'threshold': 80, 'whether_send_mail': True,
                 'whether_automatic_handle': True, 'introduction': '磁盘监控',
                 'unattended': {'Monday-Friday': ['0:00-23:59'], 'Weekend': ['0:00-23:59']}}
            ]}}
    return monitor_params



