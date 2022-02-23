#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import traceback
import json
from strategy_order.utils.const import const
from strategy_order.base.send_email import SendOrderDetailEmail
from strategy_order.utils.logger_cfg import monitor_log
from utils.redis_util import master_redis_client, report_redis_client, lock_redis_client, trade_redis_client, redis_client

logger = monitor_log


class MonitorBase:
    ''' 基类 '''

    def __init__(self):
        self.redis_client = redis_client
        self.master_redis_client = master_redis_client
        self.report_redis_client = report_redis_client
        self.lock_redis_client = lock_redis_client
        self.trade_redis_client = trade_redis_client
        self.send_detail_email = SendOrderDetailEmail()

    @staticmethod
    def get_system_param():
        system_param = {}
        try:
            system_param = master_redis_client.get(const.SYSTEM_PARAM)
            if system_param:
                system_param = json.loads(system_param)
                logger.info(f'----- system_param: {system_param} -----')
        except Exception as e:
            logger.error('----- get_system_param, {} -----'.format(traceback.format_exc()))
            print('----- get_system_param, {} -----'.format(traceback.format_exc()))
        finally:
            return system_param if system_param else {}

    @staticmethod
    def convert_to_G(size: str):
        ''' 转换成单位G '''
        try:
            if size:
                logger.info('----- 正在转换成单位G: {} -----'.format(size))
                if size[-1].upper() in ['G']:
                    return size
                elif size[-1].upper() in ['M']:
                    return str('%.2f' % (float(size[: -1]) / 1024)) + 'G'
                elif size[-1].upper() in ['K']:
                    return str('%.2f' % (float(size[: -1]) / (1024 ** 2))) + 'G'
                elif size[-1].upper() in ['B']:
                    return str('%.2f' % (float(size[: -1]) / (1024 ** 3))) + 'G'
                else:
                    return str('%.2f' % (float(size) / (1024 ** 3))) + 'G'
        except Exception as e:
            logger.error('----- convert_to_G, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def whether_unattended_period(unattended_data: dict) -> bool:
        ''' 是否无人值守时段 '''
        try:
            logger.info(f'----- 是否无人值守时段, unattended_data: {unattended_data} -----')
            localtime = time.localtime(time.time())
            cur_hour, cur_min = localtime.tm_hour, localtime.tm_min
            if 0 <= localtime.tm_wday <= 4:
                unattended_period = unattended_data.get('Monday-Friday', [])
            else:
                unattended_period = unattended_data.get('Weekend', [])

            if not unattended_period:  # 如果没有设置的话, 默认是非无人值守时段
                return False

            # 判断
            for period in unattended_period:
                pre_period_hour, pre_period_min = period.split('-')[0].split(':')[0], period.split('-')[0].split(':')[1]
                later_period_hour, later_period_min = period.split('-')[1].split(':')[0], \
                                                      period.split('-')[1].split(':')[1]

                if int(pre_period_hour) < cur_hour < int(later_period_hour):
                    logger.info('----- 无人值守时段 -----')
                    return True
                if (int(pre_period_hour) == cur_hour and cur_min > int(pre_period_min)) or (
                        int(later_period_hour) == cur_hour and cur_min < int(later_period_min)):
                    logger.info('----- 无人值守时段 -----')
                    return True

            logger.info('----- 非无人值守时段 -----')
            return False
        except Exception as e:
            logger.error('----- whether_unattended_period, {} -----'.format(traceback.format_exc()))

    def send_email(self, email_content='', email_list=None, title=''):
        try:
            self.send_detail_email.send_message(email_content, email_list, title)
            logger.info(
                '----- 发送email成功, email_content：{}, email_list: {}, title: {} ----'.format(email_content, email_list,
                                                                                           title))
        except Exception as e:
            logger.error('----- send_email, {} -----'.format(traceback.format_exc()))
