#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
import datetime
import hashlib
import traceback
import subprocess
from strategy_order.utils.const import const
from multiprocessing import Process
from utils.config import config
from strategy_order.utils.logger_cfg import monitor_log
from monitor.monitor_base import MonitorBase
from utils.function_util import get_monitor_params

logger = monitor_log


class MonitorMemorySpace(MonitorBase):
    ''' 监听内存空间 '''

    def __init__(self):
        MonitorBase.__init__(self)
        self.memoryspace_internal_time = config.monitor_memoryspace_timeinterval  # 自动监控, 间隔时间
        self.monitor_memoryspace = get_monitor_params().get('monitor_memoryspace', {})
        self.redis_space_dict = self.monitor_memoryspace.get('redis_space', {})
        self.file_space_list = self.monitor_memoryspace.get('file_space', [])
        self.service_name = config.service_name  # 当前环境
        self.shell_command = 'df -h {}'
        self.use_space_command = 'du -sh {}'  # .format(self.file_space_list.get('path', '/data'))
        self.redis_conn_dict = dict()
        self.recipients = self.get_system_param().get('maintenance_email', 'zh.zeng@wescxx.com').split(';')
        self.record_memoryspace_dict = {}  # 上一天风险记录日期, 已使用内存, 当前天风险记录日期, 已使用内存
        self.email_memoryspace_notice_record_dict = {}  # 提醒过一次, 不再提示, {'/var': {whether_recovery: ''}}
        self.init_memoryspace_record()
        self.init_redis_conn_dict()

    def init_memoryspace_record(self):
        ''' 初始化record_memoryspace_dict '''
        for redis_port in self.redis_space_dict.keys():
            self.record_memoryspace_dict['redis/{}'.format(redis_port)] = {}
            self.email_memoryspace_notice_record_dict['redis/{}'.format(redis_port)] = {}
        for file_dict in self.file_space_list:
            directory = file_dict.get('directory', '')
            self.record_memoryspace_dict['file{}'.format(directory)] = {}
            self.email_memoryspace_notice_record_dict['file{}'.format(directory)] = {}
        logger.info('----- 初始化record_memoryspace_dict完毕, {} -----'.format(self.record_memoryspace_dict))

    def init_redis_conn_dict(self):
        ''' 初始化redis_conn '''
        self.redis_conn_dict = {
            config.redis_port: self.redis_client,
            config.redis_master_port: self.master_redis_client,
            config.redis_slave_port: self.report_redis_client,
            config.redis_lock_port: self.lock_redis_client,
            config.redis_trade_port: self.trade_redis_client
        }

    @staticmethod
    def clear_empty(item):
        if item:
            return item

    def clear_redis_space(self, redis_port: int):
        ''' 清理redis内存空间'''
        try:
            redis_client_obj = self.redis_conn_dict[redis_port]
        except Exception as e:
            logger.error('----- clear_empty, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def clear_output_file():
        ''' 清理output内存空间 '''
        try:
            shell = 'cd /data/strategy/output/;  rm *.out'
            logger.info('----- 清理output内存空间, shell: {} ----- '.format(shell))
            subprocess.getstatusoutput(shell)
        except Exception as e:
            logger.error('----- clear_output_file, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def get_string_md5(string):
        ''' 获取唯一id '''
        return hashlib.md5(string.encode('utf-8')).hexdigest()

    def whether_date_equal(self, before_date: str, cur_date: str) -> bool:
        ''' 判断日期 '''
        try:
            logger.info('----- whether_date_equal, before_date: {}, cur_date: {} -----'.format(before_date, cur_date))
            return before_date.split(' ')[0] == cur_date.split(' ')[0]
        except Exception as e:
            logger.error('----- whether_date_equal, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def whether_date_differ(before_date: str, count: int) -> bool:
        ''' 是否before_date和当前日期相差count天数 '''
        cal_date = str(datetime.date.today() + datetime.timedelta(days=count))
        logger.info('----- whether_date_differ, cal_date: {}, before_date: {} -----'.format(cal_date, before_date))
        return True if cal_date == before_date else False

    def listen_file_space(self, file_space_dict, _type=None) -> dict:
        ''' 监听file磁盘空间信息 '''
        path = file_space_dict.get('directory', '')
        key = 'file{}'.format(path)
        file_id = self.get_string_md5(key)

        return_msg, tmp_dict = {}, {'type': key, 'id': file_id, 'use_size': '', 'total_size': '', 'use_percentage': '',
                                    'risk_date': '', 'last_use_size': '', 'last_risk_date': ''}
        try:
            logger.info('----- 正在监听file磁盘空间信息, key: {} -----'.format(key))

            # 查询整个空间
            output = subprocess.getstatusoutput(self.shell_command.format(path))
            result = output[1].split('\n')
            info_dict = {}
            info_name = list(filter(self.clear_empty, result[0].split(' ')))
            info_value = list(filter(self.clear_empty, result[1].split(' ')))

            # 查询使用空间
            use_space_output = subprocess.getstatusoutput(self.use_space_command.format(path))
            use_space_result = use_space_output[1].split('\t')
            use_size = use_space_result[0]

            for index, item in enumerate(info_name):
                if index < len(info_value):
                    info_dict[item] = info_value[index]
            total_size = self.convert_to_G(str(info_dict.get('Size', '')))
            use_percentage = info_dict.get('Use%', '')

            logger.info('----- 正在监听file磁盘信息, use_size: {}, total_size: {}, use_percentage: {} -----'.format(use_size,
                                                                                                            total_size,
                                                                                                            use_percentage))

            if not use_percentage:
                return_msg['success'], return_msg['datas'], return_msg['message'] = False, tmp_dict, '获取磁盘信息失败'
            else:

                cur_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                # 判断
                values = self.master_redis_client.hget(const.STRATEGY_USE_SPACE, key)
                values = json.loads(values) if values else {}
                last_use_size, last_risk_date = values.get('last_use_size', ''), values.get('last_risk_date', '')

                tmp_dict = {
                    'type': key,
                    'id': file_id,
                    'risk_level': file_space_dict.get('risk_level', ''),
                    'use_size': use_size,
                    'total_size': total_size,
                    'use_percentage': use_percentage,
                    'risk_date': cur_date,
                    'risk_notice': '环境名称: {}, 监控对象: {}, 使用率: {}'.format(self.service_name, key,
                                                                        use_percentage),
                    'last_use_size': last_use_size if self.whether_date_differ(last_risk_date.split(' ')[0],
                                                                               -1) else '',  # 从redis里取
                    'last_risk_date': last_risk_date if self.whether_date_differ(last_risk_date.split(' ')[0],
                                                                                 -1) else ''  # 从redis里取
                }
                return_msg['success'], return_msg['datas'], return_msg['message'] = True, tmp_dict, '成功获取磁盘信息'  # 返回前端

                # 记录上一天监听日期, 已使用内存
                record_risk_date = self.record_memoryspace_dict.get(key, {}).get('cur_risk_date', '')
                record_use_size = self.record_memoryspace_dict.get(key, {}).get('cur_use_size', '')

                logger.info('----- 上一次：key：{}，record_risk_date：{}，record_use_size：{} -----'.format(key,
                                                                                                   record_risk_date,
                                                                                                   record_use_size))

                # 更新
                if _type != 'query' and record_risk_date and not self.whether_date_equal(record_risk_date,
                                                                                         cur_date):  # 只有后台轮询的时候存, 前台轮询不存
                    logger.info('***************更新上一天的使用磁盘***************')
                    self.record_memoryspace_dict[key]['last_risk_date'] = record_risk_date  # 更新上一天记录时间
                    self.record_memoryspace_dict[key]['last_use_size'] = record_use_size  # 更新上一天记录磁盘

                    # 添加上一天记录日期, 记录内存, 更新
                    tmp_dict['last_risk_date'], tmp_dict['last_use_size'] = record_risk_date, record_use_size

                    # 记录过去内存状态
                    file_record_list_name = '{}.{}'.format(const.STRATEGY_USE_SPACE_RECORD, file_id)
                    self.master_redis_client.lpush(file_record_list_name, json.dumps(tmp_dict))

                    logger.info(
                        '----- 更新上一天的使用磁盘成功, 推入redis, key: {}, value: {} -----'.format(file_record_list_name, tmp_dict))

                self.record_memoryspace_dict[key]['cur_risk_date'] = cur_date  # 更新当前天记录时间
                self.record_memoryspace_dict[key]['cur_use_size'] = use_size  # 更新当前天记录磁盘

                # 更新
                values.update(tmp_dict)
                self.master_redis_client.hset(const.STRATEGY_USE_SPACE, key, json.dumps(values))

                logger.info('----- 监控file_space, 存入redis成功,{} key: {}, id: {}, value: {} -----'.format(
                    const.STRATEGY_USE_SPACE, key, file_id, values))
        except Exception as e:
            return_msg['success'], return_msg['datas'], return_msg['message'] = False, tmp_dict, traceback.format_exc()
            logger.error('----- listen_file_space, {} -----'.format(traceback.format_exc()))
        finally:
            return return_msg

    def listen_redis_space(self, redis_port: int, _type=None) -> dict:
        ''' 监听redis内存空间信息 '''
        key = 'redis/{}'.format(redis_port)
        redis_id = self.get_string_md5(key)

        return_msg, tmp_dict = {}, {'type': key, 'id': redis_id, 'use_size': '', 'total_size': '', 'use_percentage': '',
                                    'risk_date': '', 'last_use_size': '', 'last_risk_date': ''}
        try:
            logger.info('----- 正在监听redis内存空间信息, key: {} -----'.format(key))

            redis_space_dict = self.redis_space_dict.get(redis_port, {})
            logger.info('----- port: {}, self.redis_space_dict: {} -----'.format(redis_port, self.redis_space_dict))

            # 获取内存空间状态
            redis_obj = self.redis_conn_dict.get(redis_port)
            redis_info = redis_obj.info()

            # 取值
            used_memory_human, maxmemory_human = redis_info.get('used_memory_human', ''), redis_info.get(
                'maxmemory_human', '')
            used_memory, maxmemory = redis_info.get('used_memory', ''), redis_info.get(
                'maxmemory', '')

            try:
                used_memory_dataset_perc = '%.2f' % ((float(used_memory) / float(maxmemory)) * 100) + '%'
            except Exception as e:
                logger.error('----- listen_redis_space, 异常正常捕获, {} -----'.format(traceback.format_exc()))  # 异常正常捕获
                used_memory_dataset_perc = '-'

            logger.info(
                '----- 正在监听redis内存空间信息, used_memory_human:{}, maxmemory_human:{}, used_memory: {}, maxmemory: {}, used_memory_dataset_perc: {} -----'.format(
                    used_memory_human, maxmemory_human, used_memory, maxmemory, used_memory_dataset_perc))

            if not used_memory_human or not maxmemory_human:
                return_msg['success'], return_msg['datas'], return_msg['message'] = False, tmp_dict, '获取内存信息失败'
            else:
                cur_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                # 判断
                values = self.master_redis_client.hget(const.STRATEGY_USE_SPACE, key)
                values = json.loads(values) if values else {}
                last_use_size, last_risk_date = values.get('last_use_size', ''), values.get('last_risk_date', '')

                tmp_dict = {
                    'type': key,
                    'id': redis_id,
                    'risk_level': redis_space_dict.get('risk_level', ''),
                    'use_size': used_memory_human,
                    'total_size': maxmemory_human,
                    'use_percentage': used_memory_dataset_perc,
                    'risk_date': cur_date,
                    'risk_notice': '环境名称: {}, 监控对象: {}, 使用率: {}'.format(self.service_name, key,
                                                                        used_memory_dataset_perc),
                    'last_use_size': last_use_size if self.whether_date_differ(last_risk_date.split(' ')[0],
                                                                               -1) else '',  # 从redis里取
                    'last_risk_date': last_risk_date if self.whether_date_differ(last_risk_date.split(' ')[0],
                                                                                 -1) else ''  # 从redis里取
                }
                return_msg['success'], return_msg['datas'], return_msg['message'] = True, tmp_dict, '成功获取内存信息'  # 返回前端

                # 记录上一天监听日期, 已使用内存
                record_risk_date = self.record_memoryspace_dict.get(key, {}).get('cur_risk_date', '')
                record_use_size = self.record_memoryspace_dict.get(key, {}).get('cur_use_size', '')

                logger.info('----- 上一次：key：{}，record_risk_date：{}，record_use_size：{} -----'.format(key,
                                                                                                   record_risk_date,
                                                                                                   record_use_size))
                # 更新
                if _type != 'query' and record_risk_date and not self.whether_date_equal(record_risk_date,
                                                                                         cur_date):  # 只有后台轮询的时候存, 前台轮询不存
                    logger.info('***************更新上一天的使用内存***************')
                    self.record_memoryspace_dict[key]['last_risk_date'] = record_risk_date  # 更新上一天记录时间
                    self.record_memoryspace_dict[key]['last_use_size'] = record_use_size  # 更新上一天记录内存

                    # 添加上一天记录日期, 记录内存, 更新
                    tmp_dict['last_risk_date'], tmp_dict['last_use_size'] = record_risk_date, record_use_size

                    # 记录过去内存状态
                    redis_record_list_name = '{}.{}'.format(const.STRATEGY_USE_SPACE_RECORD, redis_id)
                    self.master_redis_client.lpush(redis_record_list_name, json.dumps(tmp_dict))

                    logger.info('----- 更新上一天的使用内存成功, 推入redis, key: {}, value: {} -----'.format(redis_record_list_name,
                                                                                               tmp_dict))

                self.record_memoryspace_dict[key]['cur_risk_date'] = cur_date  # 更新当前天记录时间
                self.record_memoryspace_dict[key]['cur_use_size'] = used_memory_human  # 更新当前天记录内存

                # 更新
                values.update(tmp_dict)
                self.master_redis_client.hset(const.STRATEGY_USE_SPACE, key, json.dumps(values))

                logger.info('----- 监控redis_space, 存入redis成功,{} key: {}, id: {}, value: {} -----'.format(
                    const.STRATEGY_USE_SPACE, key, redis_id, values))
        except Exception as e:
            return_msg['success'], return_msg['datas'], return_msg['message'] = False, tmp_dict, traceback.format_exc()
            logger.error('----- listen_redis_space, {} -----'.format(traceback.format_exc()))
        finally:
            return return_msg

    def automatic_monitor_file_space(self, file_obj):
        ''' 监控file磁盘空间信息, 自动处理 '''
        try:
            risk_level, threshold = file_obj.get('risk_level', None), file_obj.get('threshold', None)
            if not risk_level or not threshold:
                return

            logger.info(
                '----- 正在自动监控file_space, key: file{}, risk_level: {}, threshold: {}, memoryspace_internal_time: {}, 是否发送邮件: {} -----'.format(
                    file_obj.get('directory', ''), risk_level, threshold, self.memoryspace_internal_time,
                    file_obj.get('whether_send_mail', '')))

            return_msg = self.listen_file_space(file_obj)
            if not return_msg.get('success', ''):  # 如果异常,无需继续执行
                print('----- 自动监控file_space, 监控对象: file, 异常返回详情: {} ------'.format(return_msg))
                logger.info(
                    '----- 自动监控file_space, 监控对象: file{}, 异常返回详情: {} ------'.format(file_obj.get('directory', ''),
                                                                                   return_msg))
                return

            datas = return_msg.get('datas', {})
            use_percentage = datas.get('use_percentage', '')  # 50%或-

            if use_percentage == '-':  # 无需往下执行, -
                return

            key = 'file{}'.format(file_obj.get('directory'))
            if float(use_percentage.split('%')[0]) > threshold:
                logger.info('----- 正在自动监控file_space, key: file{}, 大于边界阈值, 监听阈值: {}, 边界阈值: {} -----'.format(
                    file_obj.get('directory', ''), float(use_percentage.split('%')[0]), threshold))

                if risk_level == 3:  # 高风险
                    if self.whether_unattended_period(file_obj.get('unattended', {})):  # 无人值守
                        self.clear_output_file()

                    if file_obj.get('whether_send_mail', ''):
                        whether_recovery = self.email_memoryspace_notice_record_dict[key].get('whether_recovery',
                                                                                              None)  # 上一次恢复记录. 默认首次None

                        if whether_recovery == None or whether_recovery:  # 首次或者已经恢复, 提醒一次
                            logger.info(
                                '----- 磁盘使用率高于阈值且已恢复一次, 监控对象: {}, self.email_memoryspace_notice_record_dict: {}, 发送提醒邮件 -----'.format(
                                    key, self.email_memoryspace_notice_record_dict))

                            self.send_email(datas.get('risk_notice', ''), self.recipients, '磁盘使用提醒')  # 磁盘超出, 邮件提醒一次
                            self.email_memoryspace_notice_record_dict[key]['whether_recovery'] = False  # 更新
            else:
                whether_recovery = self.email_memoryspace_notice_record_dict[key].get('whether_recovery',
                                                                                      True)  # 上一次恢复记录. 默认上次 True
                if file_obj.get('whether_send_mail', '') and not whether_recovery:
                    logger.info(
                        '----- 磁盘使用率低于阈值发送当前使用率恢复邮件, 监控对象: {}, self.email_memoryspace_notice_record_dict: {}, 发送恢复邮件 -----'.format(
                            key, self.email_memoryspace_notice_record_dict))

                    self.send_email(datas.get('risk_notice', ''), self.recipients, '磁盘恢复提醒')  # 磁盘恢复, 邮件提醒一次
                    self.email_memoryspace_notice_record_dict[key]['whether_recovery'] = True  # 更新
            return True
        except Exception as e:
            logger.error('----- automatic_monitor_file_space, {} -----'.format(traceback.format_exc()))

    def automatic_monitor_redis_space(self, redis_port: int):
        ''' 监控redis内存空间信息, 自动处理 '''
        try:
            values = self.redis_space_dict.get(redis_port, {})
            risk_level, threshold = values.get('risk_level', None), values.get('threshold', None)
            if not risk_level or not threshold:
                return

            logger.info(
                '----- 正在自动监控redis_space, risk_level: {}, threshold: {}, memoryspace_internal_time: {}, 是否发送邮件: {} -----'.format(
                    risk_level, threshold, self.memoryspace_internal_time,
                    values.get('whether_send_mail', '')))

            return_msg = self.listen_redis_space(redis_port)
            if not return_msg.get('success', ''):  # 如果异常,无需继续执行
                print('----- 自动监控redis_space, 监控对象: {}, 异常返回详情: {} -----'.format(redis_port, return_msg))
                logger.info('----- 自动监控redis_space, 监控对象: redis/{}, 异常返回详情: {} -----'.format(redis_port, return_msg))
                return

            datas = return_msg.get('datas', {})
            use_percentage = datas.get('use_percentage', '')  # 34.32%或-

            if use_percentage == '-':  # 无需往下执行, -
                return

            key = 'redis/{}'.format(redis_port)
            if float(use_percentage.split('%')[0]) > threshold:
                logger.info('----- 正在自动监控redis_space, key: redis/{}, 大于边界阈值, 监听阈值: {}, 边界阈值: {} -----'.format(
                    redis_port, float(use_percentage.split('%')[0]), threshold))

                if risk_level == 3:  # 高风险
                    if self.whether_unattended_period(values.get('unattended', {})):  # 无人值守
                        self.clear_redis_space(redis_port)

                    if values.get('whether_send_mail', ''):
                        whether_recovery = self.email_memoryspace_notice_record_dict[key].get('whether_recovery',
                                                                                              None)  # 上一次恢复记录, 默认首次False
                        if whether_recovery == None or whether_recovery:  # 首次或者已经恢复, 提醒一次
                            logger.info(
                                '----- 内存使用率高于阈值且已恢复一次, 监控对象: {}, self.email_memoryspace_notice_record_dict: {}, 发送提醒邮件 -----'.format(
                                    key, self.email_memoryspace_notice_record_dict))

                            self.send_email(datas.get('risk_notice', ''), self.recipients, '内存使用提醒')  # 内存超出, 邮件提醒一次

                            self.email_memoryspace_notice_record_dict[key]['whether_recovery'] = False  # 更新
            else:
                whether_recovery = self.email_memoryspace_notice_record_dict[key].get('whether_recovery',
                                                                                      True)  # 上一次恢复记录, 默认上次True

                if values.get('whether_send_mail', '') and not whether_recovery:
                    logger.info(
                        '----- 内存使用率低于阈值发送当前使用率恢复邮件, 监控对象: {}, self.email_memoryspace_notice_record_dict: {}, 发送恢复邮件 -----'.format(
                            key, self.email_memoryspace_notice_record_dict))

                    self.send_email(datas.get('risk_notice', ''), self.recipients, '内存恢复提醒')  # 内存恢复, 邮件提醒一次
                    self.email_memoryspace_notice_record_dict[key]['whether_recovery'] = True  # 更新
            return True
        except Exception as e:
            logger.error('----- automatic_monitor_redis_space, {} -----'.format(traceback.format_exc()))

    def monitor_memoryspace_start(self):
        ''' 启动内存空间监控 '''
        while True:
            for redis_port in self.redis_space_dict.keys():
                if self.redis_space_dict.get(redis_port, {}).get('whether_automatic_handle', ''):  # 判断是否自动处理
                    logger.info('----- 正在监控redis: {}内存空间信息 -----'.format(redis_port))
                    self.automatic_monitor_redis_space(int(redis_port))
            if self.file_space_list:
                for file_obj in self.file_space_list:
                    if file_obj.get('whether_automatic_handle', ''):  # 判断是否自动处理
                        logger.info('----- 正在监控file内存空间信息 -----')
                        self.automatic_monitor_file_space(file_obj)

            time.sleep(self.memoryspace_internal_time)

    def process_monitor_memoryspace_start(self):
        ''' 开启进程,自动监控memoryspace '''
        try:
            logger.info('------ 开启进程,自动监控memoryspace -----')
            print('----- 开启进程,自动监控memoryspace -----')
            p = Process(target=self.monitor_memoryspace_start)
            p.run()
            p.join()
        except Exception as e:
            logger.error('----- process_monitor_memoryspace_start, {} -----'.format(traceback.format_exc()))


if __name__ == '__main__':
    obj = MonitorMemorySpace()
    # obj.process_monitor_memoryspace_start()
    print(obj.get_string_md5('redis/6380'))