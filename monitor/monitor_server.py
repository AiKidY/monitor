# !/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import json
import time
import redis
import traceback
import pymysql
import subprocess
from multiprocessing import Process
from utils.config import config
from strategy_order.utils.logger_cfg import monitor_log
from monitor.monitor_listen_port import ListenPort
from os.path import dirname, abspath
from strategy_order.base.strategy_scheduler import StrategyScheduler
from strategy_order.utils.const import const
from monitor.monitor_base import MonitorBase
from utils.function_util import get_monitor_params

logger = monitor_log


class MonitorServer(MonitorBase):
    ''' 监控redis和mysql服务器 '''

    def __init__(self):
        MonitorBase.__init__(self)
        self.redis_service = {}
        self.mysql_service = {}
        self.port_server_file_dict = {}
        self.record_monitor_time_history_dict = {}  # 服务状态历史记录
        self.service_name = config.service_name  # 当前环境
        self.monitorserver_internal_time = config.monitor_port_timeinterval
        self.redis_root_path = config.redis_root_path
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self.redis_project_path = self.get_project_path() + '/redis-cluster'
        self.recipients = self.get_system_param().get('maintenance_email', 'zh.zeng@wescxx.com').split(';')
        self.scheduler_dict = {'scheduler_type': 'monitor_job', 'scheduler_jobs': ['automatic_listen_port_job']}
        self.email_server_notice_record_dict = {}  # 服务状态邮件提醒恢复记录, {3306: True}
        self.monitor_port = get_monitor_params().get('monitor_port', {})  # 要监听的端口以及端口对应的配置文件
        self.listen_port = ListenPort()
        self.strategy_scheduler = StrategyScheduler(self.scheduler_dict)
        self.init_redis_coon()
        self.init_mysql_coon()
        self.init_port_server_file_dict()

    def init_redis_coon(self):
        try:
            self.redis_conn_dict = {
                config.redis_port: self.redis_client,
                config.redis_master_port: self.master_redis_client,
                config.redis_slave_port: self.report_redis_client,
                config.redis_lock_port: self.lock_redis_client,
                config.redis_trade_port: self.trade_redis_client
            }
            for port, conn_redis in self.redis_conn_dict.items():
                self.redis_service[port] = conn_redis
                self.record_monitor_time_history_dict[port] = {}
        except Exception as e:
            logger.error('----- init_redis_coon, {} -----'.format(traceback.format_exc()))

    def init_redis_coon_bak(self):
        redis_obj = config.redis
        default_dict = {}
        for key in list(redis_obj.keys()):
            value = redis_obj[key]
            if not isinstance(value, dict):
                default_dict[key] = value
                del redis_obj[key]
        redis_obj['default'] = default_dict
        ports = self.monitor_port.keys()
        for key, value in redis_obj.items():
            host, port, password, db = value['host'], value['port'], value['password'], value['db']
            if port in ports:
                try:
                    conn_redis = redis.Redis(host=host,
                                             port=port,
                                             password=password,
                                             db=db)
                    self.redis_service[port] = conn_redis
                    self.record_monitor_time_history_dict[port] = {}
                except Exception as e:
                    logger.error('----- {}_{}_{} -----'.format(host, port, traceback.format_exc()))

    def init_mysql_coon(self):
        try:
            ports = self.monitor_port.keys()
            for key, value in config.mysql_obj.items():
                host, port, password, user, db, charset = value.get('host', ''), value.get('port', ''), value.get(
                    'password', ''), value.get('user', ''), value.get('db', ''), value.get('charset', '')
                if port in ports:
                    try:
                        self.mysql_service[port] = value
                        self.record_monitor_time_history_dict[port] = {}
                    except Exception as e:
                        logger.error('----- {}_{}_{} -----'.format(host, port, traceback.format_exc()))
        except Exception as e:
            logger.error('----- init_mysql_coon -----'.format(traceback.format_exc()))

    def init_port_server_file_dict(self):
        ''' 初始化端口服务文件 '''
        try:
            self.port_server_file_dict[config.zmq_rep_port] = ['strategy_deal_recv.py']
            self.port_server_file_dict[config.zmq_positions_port] = ['strategy_position_store.py',
                                                                     'strategy_position_push.py']
            self.port_server_file_dict[config.zmq_assets_port] = ['strategy_asset_push.py',
                                                                  'strategy_asset_store.py']

            # 初始化服务状态历史记录
            for port in self.listen_port.ping_port_array:
                self.record_monitor_time_history_dict[port] = {}
            logger.info('----- 初始化端口服务文件完毕: {} -----'.format(self.port_server_file_dict))
        except Exception as e:
            logger.error('----- init_port_server_file_dict, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def get_project_path():
        dir_path = dirname(dirname(abspath(__file__)))
        dir_paths = dir_path.split('/')
        last_index = 0
        for index, p_name in enumerate(dir_paths):
            if re.search(config.project_name, p_name):
                last_index = index
                break
        return '/'.join(dir_paths[0: last_index])

    def record_monitor_time(self, port: int):
        ''' 记录监听时间 '''
        try:
            operation_dict = self.master_redis_client.hget(const.SERVICE_OPERATION_RECORD, port)
            monitor_time = int(time.time() * 1000000)
            if operation_dict:
                operation_dict = json.loads(operation_dict.decode('utf-8'))
                operation_dict['monitor_time'] = monitor_time
            else:
                operation_dict = {
                    'operator': '',
                    'operation_time': '',
                    'monitor_time': monitor_time
                }
            self.master_redis_client.hset(const.SERVICE_OPERATION_RECORD, port, json.dumps(operation_dict))
        except Exception as e:
            logger.error('----- record_monitor_time, {} -----'.format(traceback.format_exc()))

    def monitor_server_start(self):
        ''' 启动监控 '''
        ports = self.monitor_port.keys()

        # 开启定时任务,监听端口服务是否启动
        self.strategy_scheduler.start_scheduler_job()

        while True:
            # 启动监控redis服务
            for port, coon in self.redis_service.items():
                logger.info('----- 正在监听port_{} -----'.format(port))
                self.monitor_redis_server(port, coon)

            # 启动监控mysql服务
            for port, coon_dict in self.mysql_service.items():
                logger.info('----- 正在监听port_{} -----'.format(port))
                self.monitor_mysql_server(port, coon_dict)

            for port in self.listen_port.ping_port_array:
                if port in ports:
                    # 启动端口断开自动重启
                    logger.info('----- 正在监听port_{} -----'.format(port))
                    self.monitor_port_server(port)

            logger.info('----- 服务状态历史记录, record_monitor_time_history_dict: {} -----'.format(
                self.record_monitor_time_history_dict))  # 服务状态历史记录
            logger.info('----- 服务状态邮件提醒恢复记录, email_server_notice_record_dict: {} -----'.format(
                self.email_server_notice_record_dict))
            time.sleep(self.monitorserver_internal_time)

    def monitor_redis_server(self, port: int, coon=None, _type=None) -> bool:
        ''' 监控 redis_server '''
        server_dict, server_status = self.monitor_port.get(port, {}), None
        try:
            coon = coon if coon else self.redis_service.get(port, None)

            if not coon:
                server_status = False
            else:
                coon.ping()
                server_status = True

                whether_recovery = self.email_server_notice_record_dict.get(port, True)  # 上一次恢复记录. 默认上次 True
                if not whether_recovery:  # 恢复发送一次
                    self.send_email('环境名称: {}, 监控端口: {}, redis服务恢复!'.format(self.service_name, port), self.recipients,
                                    'redis服务恢复提醒')
                    self.email_server_notice_record_dict[port] = True  # 恢复更新
                logger.info('----- 成功连接redis_server_{} -----'.format(port))
        except Exception as e:
            logger.error('----- monitor_redis_server, {} -----'.format(traceback.format_exc()))
            if _type != 'query':
                # 发送提醒邮件
                if server_dict.get('whether_send_mail', ''):
                    whether_recovery = self.email_server_notice_record_dict.get(port, None)
                    if whether_recovery == None or whether_recovery == True:
                        self.send_email('环境名称: {}, 监控端口: {}, reids服务中断,请及时处理!'.format(self.service_name, port),
                                        self.recipients, 'redis服务中断提醒')
                        self.email_server_notice_record_dict[port] = False  # 中断更新

                # 是否无人值守
                if self.whether_unattended_period(server_dict.get('unattended', {})):
                    if server_dict.get('whether_restart', ''):
                        logger.info('----- 无人值守时段,正在重启Redis服务: {} -----'.format(port))
                        self.restart_redis_server(port)
                    else:
                        logger.info('----- whether_restart = False,不重启Redis服务: {} -----'.format(port))
                else:
                    logger.info('----- 非无人值守时段,不重启Redis服务: {} -----'.format(port))

            # 监控告警, 存redis
            if server_dict.get('whether_monitor_alarm', ''):
                self.monitor_server_alarm(port)
            server_status = False
        finally:
            # 记录最近一次的更新监听时间, 后端轮询记录
            if _type != 'query':
                last_server_status = self.record_monitor_time_history_dict.get(port, {}).get('last_server_status', None)
                if last_server_status == False and not server_status:
                    logger.info(
                        '----- 不更新最近一次的监听时间, 监听服务: {}, 上一次服务状态: {}, 当前服务状态: {} -----'.format(port, last_server_status,
                                                                                             server_status))
                else:
                    logger.info('----- 更新最近一次的监听时间 -----')
                    self.record_monitor_time(port)
                self.record_monitor_time_history_dict[port]['last_server_status'] = server_status
            return server_status

    def monitor_mysql_server(self, port: int, conn_dict=None, _type=None) -> bool:
        ''' 监控 mysql_server '''
        server_dict, server_status = self.monitor_port.get(port, {}), None
        try:
            conn_dict = conn_dict if conn_dict else self.mysql_service.get(port, {})

            if not conn_dict:
                server_status = False
            else:
                conn = pymysql.connect(host=conn_dict['host'], port=conn_dict['port'], user=conn_dict['user'],
                                       password=conn_dict['password'], db=conn_dict['db'], charset=conn_dict['charset'])
                conn.ping()

                whether_recovery = self.email_server_notice_record_dict.get(port, True)  # 上一次恢复记录. 默认上次 True
                if not whether_recovery:  # 恢复发送一次
                    self.send_email('环境名称: {}, 监控端口: {}, mysql服务恢复!'.format(self.service_name, port), self.recipients,
                                    'mysql服务恢复提醒')
                    self.email_server_notice_record_dict[port] = True  # 恢复更新
                logger.info('----- 成功连接mysql_server_{} -----'.format(port))
                server_status = True
        except Exception as e:
            logger.error('----- monitor_mysql_server, {} -----'.format(traceback.format_exc()))
            if _type != 'query':
                # 发送提醒邮件
                if server_dict.get('whether_send_mail', ''):
                    whether_recovery = self.email_server_notice_record_dict.get(port, None)
                    if whether_recovery == None or whether_recovery == True:
                        self.send_email('环境名称: {}, 监控端口: {}, mysql服务中断,请及时处理!'.format(self.service_name, port),
                                        self.recipients, 'mysql服务中断提醒')
                        self.email_server_notice_record_dict[port] = False  # 中断更新

                # 是否无人值守
                if self.whether_unattended_period(server_dict.get('unattended', {})):
                    if server_dict.get('whether_restart', ''):
                        logger.info('----- 无人值守时段,正在重启Mysql服务: {} -----'.format(port))
                        self.restart_mysql_server(port)
                    else:
                        logger.info('----- whether_restart = False, 不重启Mysql服务: {} -----'.format(port))
                else:
                    logger.info('----- 非无人值守时段, 不重启Mysql服务: {} -----'.format(port))

            # 监控告警, 存redis
            if server_dict.get('whether_monitor_alarm', ''):
                self.monitor_server_alarm(port)
            server_status = False
        finally:
            # 记录最近一次的更新监听时间, 后端轮询记录
            if _type != 'query':
                last_server_status = self.record_monitor_time_history_dict.get(port, {}).get('last_server_status', None)
                if last_server_status == False and not server_status:
                    logger.info(
                        '----- 不更新最近一次的监听时间, 监听服务: {}, 上一次服务状态: {}, 当前服务状态: {} -----'.format(port, last_server_status,
                                                                                             server_status))
                else:
                    logger.info('----- 更新最近一次的监听时间 -----')
                    self.record_monitor_time(port)
                self.record_monitor_time_history_dict[port]['last_server_status'] = server_status
            return server_status

    def monitor_port_server(self, port: int, _type=None) -> bool:
        ''' 监控其他端口服务 资产、持仓 '''
        server_status, server_dict = None, self.monitor_port.get(port, {})
        try:
            logger.info('----- 正在监控{}端口 -----'.format(port))

            # 前端轮询
            if _type == 'query':
                if port in self.listen_port.ping_port_array:
                    server_status = self.listen_port.ping_port(port)
                else:
                    server_status = False
            # 后端轮询
            else:
                port_name = ''
                if port == int(config.zmq_assets_port):
                    port_name = '资产'
                elif port == int(config.zmq_positions_port):
                    port_name = '持仓'

                # 判断端口是否通
                if not self.strategy_scheduler.port_status_dict.get(port, None):
                    server_status = False

                    # 发送提醒邮件
                    if server_dict.get('whether_send_mail', ''):
                        whether_recovery = self.email_server_notice_record_dict.get(port, None)  # 是否恢复
                        if whether_recovery == None or whether_recovery == True:  # 首次/已恢复
                            self.send_email(
                                '环境名称: {}, 监控端口: {}, {}服务中断,请及时处理!'.format(self.service_name, port, port_name),
                                self.recipients, '{}服务中断提醒'.format(port_name))
                            self.email_server_notice_record_dict[port] = False  # 中断更新

                    # 是否无人值守
                    if self.whether_unattended_period(server_dict.get('unattended', {})):
                        if server_dict.get('whether_restart', ''):
                            logger.info('----- 无人值守时段,正在重启{}端口服务 -----'.format(port))
                            self.restart_port_server(port)
                        else:
                            logger.info('----- whether_restart = False,不重启{}端口服务 -----'.format(port))
                    else:
                        logger.info('----- 非无人值守时段, 不重启{}端口服务 -----'.format(port))
                else:
                    server_status = True

                    whether_recovery = self.email_server_notice_record_dict.get(port, True)  # 上一次恢复记录. 默认上次 True
                    if not whether_recovery:  # 恢复发送一次
                        self.send_email('环境名称: {}, 监控端口: {}, {}服务恢复!'.format(self.service_name, port, port_name),
                                        self.recipients, '{}服务恢复提醒'.format(port_name))
                        self.email_server_notice_record_dict[port] = True  # 恢复更新

            # 判断
            if not server_status:
                # 监控告警, 存redis
                if server_dict.get('whether_monitor_alarm', ''):
                    self.monitor_server_alarm(port)
            else:
                logger.info('----- {}端口启动成功 -----'.format(port))
        except Exception as e:
            logger.error('----- monitor_port_server, {} -----'.format(traceback.format_exc()))
        finally:
            # 记录最近一次的更新监听时间, 后端轮询记录
            if _type != 'query':
                last_server_status = self.record_monitor_time_history_dict.get(port, {}).get('last_server_status', None)
                if last_server_status == False and not server_status:
                    logger.info(
                        '----- 不更新最近一次的监听时间, 监听服务: {}, 上一次服务状态: {}, 当前服务状态: {} -----'.format(port, last_server_status,
                                                                                             server_status))
                else:
                    logger.info('----- 更新最近一次的监听时间 -----')
                    self.record_monitor_time(port)
                self.record_monitor_time_history_dict[port]['last_server_status'] = server_status
            return server_status

    def restart_redis_server(self, port: int):
        '''重启redis_server'''
        try:
            path = self.monitor_port.get(port, {}).get('path', '')
            if not path:
                return
            logger.info('----- 正在重启redis_server -----')
            shell = 'cd {}; ./redis-server {}/{}'.format(self.redis_root_path, self.redis_project_path, path)
            logger.info('----- shell：{} -----'.format(shell))
            os.system(shell)
        except Exception as e:
            logger.error('----- restart_redis_server, {} -----'.format(traceback.format_exc()))
        else:
            logger.info('----- redis_server启动成功 -----')

    def restart_mysql_server(self, port: int):
        '''重启mysql_server'''
        try:
            path = self.monitor_port.get(port, {}).get('path', '')
            logger.info('----- restart_mysql_server_path：{} -----'.format(path))
            if not path:
                return
            logger.info('----- 正在重启mysql_server -----')
            shell = 'cd {}; ./bin/mysqld_safe --defaults-file=./my.cnf&'.format(path)
            logger.info('shell：{}'.format(shell))
            os.system(shell)
        except Exception as e:
            logger.error('----- restart_mysql_server, {} -----'.format(traceback.format_exc()))
        else:
            logger.info('----- mysql_server启动成功 -----')

    def restart_port_server(self, port: int):
        ''' 重启端口服务 '''
        restart_status = None
        try:
            logger.info('----- 正在重启{}端口 -----'.format(port))
            if port not in self.port_server_file_dict.keys():
                return False

            dir_path = dirname(dirname(abspath(__file__)))  # 根目录

            file_name_array = self.port_server_file_dict.get(port, [])
            for file_name in file_name_array:
                if re.search('strategy_new', dir_path):
                    port_path = '/data/strategy_new/digital-currency-strategy-trade/{}'.format(file_name)
                else:
                    port_path = '/data/strategy/digital-currency-strategy-trade/{}'.format(file_name)
                result = subprocess.getstatusoutput('ps aux|grep python3')[1]
                for item in result.split('\n'):
                    if re.search(port_path, item):
                        pid = [val for val in item.split(' ') if val][1]
                        logger.info('----- 重启端口: {}, pid：{} -----'.format(port, pid))
                        subprocess.getstatusoutput('kill -9 {}'.format(pid))
                        logger.info('----- 重启[{}]端口成功 -----'.format(port))
                        restart_status = True
                        break
                else:
                    restart_status = False
                    logger.info('----- 重启[{}]端口失败, 未找到对应服务 -----'.format(port))
                    break  # 一次重启失败, 直接退出
        except Exception as e:
            restart_status = False
            logger.error('----- restart_port_server, {} -----'.format(traceback.format_exc()))
        finally:
            return restart_status

    def process_monitor_server_start(self):
        '''开启进程,监控server'''
        try:
            logger.info('----- 开启进程,监控server -----')
            p = Process(target=self.monitor_server_start)
            p.run()
            p.join()
        except Exception as e:
            logger.error('----- process_monitor_server_start, {} -----'.format(traceback.format_exc()))

    def ping(self, port: int, _type=None):
        ''' ping端口服务 '''
        if port in self.mysql_service.keys():  # mysql服务
            status = self.monitor_mysql_server(port, None, _type)
        elif port in self.redis_service.keys():  # redis服务
            status = self.monitor_redis_server(port, None, _type)
        else:  # 其他端口服务
            status = self.monitor_port_server(port, _type)

        if _type == 'query':
            message = '服务正常!' if status else '服务无法连接,请重启服务!'
        else:
            if self.whether_unattended_period(self.monitor_port.get(port, {}).get('unattended', {})):
                message = '服务正常!' if status else '{}服务无法连接,已重启服务,请稍后查看结果!'.format(port)
            else:
                message = '服务正常!' if status else '服务无法连接,请重启服务!'

        return {'success': status, 'message': message}

    def monitor_server_alarm(self, port: int):
        ''' 监控server告警, 存redis '''
        if port:
            try:
                monitor_alarm_key = 'server_{}_alarm'.format(port)
                port_introduction = self.monitor_port.get(port, '').get('port_introduction', '')
                alarm_name = port_introduction.split('服务')[0] + '服务'

                monitor_alarm_value = {
                    'alarm_port': port,
                    'alarm_date': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    'alarm_detail': '{}运行端口({})无法连接,请联系管理员进行处理'.format(alarm_name, port)
                }

                self.master_redis_client.hset(const.STRATEGY_MONITOR_ALARM, monitor_alarm_key,
                                              json.dumps(monitor_alarm_value))
                logger.info(
                    '----- 监控port: {}告警, monitor_alarm_value: {} -----'.format(port, json.dumps(monitor_alarm_value)))
                print('----- 监控port: {}告警, monitor_alarm_value: {} -----'.format(port, json.dumps(monitor_alarm_value)))
            except Exception as e:
                logger.error('----- monitor_server_alarm, {} -----'.format(traceback.format_exc()))


if __name__ == '__main__':
    obj = MonitorServer()
    result = obj.process_monitor_server_start()
