#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import json
import time
import traceback
import subprocess
from os.path import dirname, abspath
from strategy_order.utils.const import const
from multiprocessing import Process
from utils.config import config
from strategy_order.utils.logger_cfg import monitor_log
from monitor.monitor_base import MonitorBase

logger = monitor_log


class MonitorStrategyProcess(MonitorBase):
    ''' 监控strategy进程 '''

    def __init__(self):
        MonitorBase.__init__(self)
        self.process_shell = 'ps aux|grep python3'
        self.monitorprocess_internal_time = config.monitor_process_timeinterval  # 间隔时间
        self.config_monitorprocesses = config.demon_monitorprocesses  # 配置文件守护进程
        self.service_name = config.service_name  # 当前环境
        self.strategyprocesses = list()  # 已经启动的进程
        self.restart_strategy_process_interval_time = 60 * 5  # 重启进程间隔判断时间
        self.restart_strategy_process_count = 3  # 重启进程间隔次数
        self.recipients = self.get_system_param().get('maintenance_email', 'zh.zeng@wescxx.com').split(';')
        self.email_process_notice_record_dict = {}  # 提醒过一次, 不再提示, {'start_strategy_scheduler': True}
        self.whether_send_email = config.monitor_process_send_email  # True 是 False 否

    def get_init_datas(self):
        ''' 初始化datas '''
        tmp_dict = {
            'user': '',
            'pid': '',
            'cpu': '',  # 0.1%
            'mem': '',  # 0.5%
            'vsz': '',
            'rss': '',
            'stat': '',
            'start': '',
            'time': '',
            'command': '',
        }
        return tmp_dict

    def strategy_process_sort(self, datas: list, sort_type, sort_rule) -> list:
        ''' 进程排序 sort_rule:  ascending升序  descending降序'''
        if not sort_rule:
            return datas

        # 升序
        if sort_type == 'cpu':
            datas.sort(key=lambda x: float(x.get('cpu', '0').split('%')[0]))
        elif sort_type == 'mem':
            datas.sort(key=lambda x: float(x.get('mem', '0').split('%')[0]))

        return datas if sort_rule == 'ascending' else datas[:: -1]

    def listen_strategy_process(self, sort_type=None, sort_rule=None, whether_command_name=False) -> dict:
        ''' 监听strategy进程, whether_command_name=True 针对后台轮询 '''
        return_msg, tmp_dict = {}, self.get_init_datas()
        try:
            logger.info(
                '----- listen_strategy_process, sort_type: {}, sort_rule: {} -----'.format(sort_type, sort_rule))
            tmp_tuple = subprocess.getstatusoutput(self.process_shell)
            # tmp_tuple = (0,
            #              'root      2071 27.0  0.0 239916 22168 pts/4    S+   10:09   0:00 python3 queryRedisList.py\nroot      2143  0.0  0.0 113280  1192 pts/4    S+   10:09   0:00 /bin/sh -c ps aux|grep python3\nroot      2145  0.0  0.0 112812   948 pts/4    S+   10:09   0:00 grep python3\nroot      3030  0.4  0.0 353700 20088 ?        Sl   Jan18  59:26 python3 /data/strategy/digital-currency-strategy-trade/strategy_asset_store.py\nroot      3737  2.0  0.0 477700 74940 ?        Sl   Jan26  20:43 python3 /data/strategy/digital-currency-strategy-trade/strategy_position_push.py\nroot      3972 14.5  0.0 354152 23188 ?        Sl   Jan26 145:57 python3 /data/strategy/digital-currency-strategy-trade/strategy_position_store.py\nroot     10255  3.6  0.0 8663272 71880 ?       Sl   Jan14 672:10 python3 /data/strategy/digital-currency-strategy-trade/strategy_spot_storage.py\nroot     10451 19.1  0.2 3335948 263268 ?      Sl   Jan14 3489:39 python3 /data/strategy/digital-currency-strategy-trade/strategy_storage.py\nroot     10577  9.0  0.1 8743396 201608 ?      Sl   Jan14 1658:36 python3 /data/strategy/digital-currency-strategy-trade/strategy_generator_hb.py\nroot     10724  8.5  0.1 8718560 145068 ?      Sl   Jan14 1552:55 python3 /data/strategy/digital-currency-strategy-trade/strategy_generator_ok.py\nroot     11164  0.8  0.0 311940 14404 ?        Sl   Jan14 147:30 python3 /data/strategy/digital-currency-strategy-trade/strategy_cal2store_broker.py\nroot     12070  0.0  0.0 246792 24120 ?        S    Jan14   1:05 python3 /data/strategy/digital-currency-strategy-trade/strategy_refresh.py\nroot     13095  0.0  0.0 832680 22192 ?        Sl   Jan14   2:18 python3 /data/strategy/digital-currency-strategy-trade/strategy_deal.py\nroot     13563  0.0  0.2 2799976 386736 ?      Sl   Jan14   7:16 python3 /data/strategy/digital-currency-strategy-trade/strategy_http_server.py\nroot     14632  0.0  0.0 574980 21744 ?        Sl   Jan14  15:04 python3 /data/strategy/digital-currency-strategy-trade/strategy_deal_recv.py\nroot     14918  0.0  0.0 222768 16868 ?        S    Jan14   1:54 python3 /data/strategy/digital-currency-strategy-trade/strategy_order_entrust.py\nroot     15721  0.0  0.0 1249940 40756 ?       Sl   Jan14  13:24 python3 /data/strategy/digital-currency-strategy-trade/strategy_monitor_server.py\nroot     16035  0.0  0.0 1249356 43364 ?       Sl   Jan14  12:16 python3 /data/strategy/digital-currency-strategy-trade/start_strategy_scheduler.py\nroot     18222  0.0  0.0 1060024 19372 ?       Sl   Jan26   0:28 python3 /data/strategy/digital-currency-strategy-trade/time_node_console.py\nroot     18596  0.0  0.0 228388 18344 ?        S    Jan14  12:41 python3 /data/strategy/digital-currency-strategy-trade/strategy_monitor_memoryspace.py\nroot     20273  1.9  5.1 7031220 6641252 ?     Sl   Jan25  51:25 python3 /data/strategy/digital-currency-strategy-trade/strategy_push_zmq.py')  # 测试

            return_msg['code'], result = 200, list()

            # 判断
            if tmp_tuple[0]:
                return_msg['success'], return_msg['datas'], return_msg[
                    'message'] = False, tmp_dict if not whether_command_name else result, tmp_tuple[1]
            else:
                process_monitor_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                for item in tmp_tuple[1].split('\n'):
                    detail = [val for val in item.split(' ') if val]
                    if re.search('strategy', detail[-1]) and not re.search('strategy_new', detail[-1]):
                        command_name = detail[11].split('/')[-1]

                        if whether_command_name:  # 针对配置文件守护进程和strategy进程比对
                            result.append(command_name.split('.py')[0])
                        else:  # 前端返回
                            tmp_dict = {
                                'user': detail[0],
                                'pid': detail[1],
                                'cpu': '{}'.format(detail[2]),
                                'mem': '{}'.format(detail[3]),
                                'vsz': detail[4],
                                'rss': detail[5],
                                'stat': detail[7],
                                'start': detail[8],
                                'time': detail[9],
                                'command': command_name.split('.py')[0],
                                'monitor_date': process_monitor_date
                            }
                            result.append(tmp_dict)

                            # 存redis
                            self.master_redis_client.hset(const.STRATEGY_PROCESS, command_name, json.dumps(tmp_dict))

                            logger.info(
                                '----- listen_strategy_process, 保存redis成功, reids_key: {}, process_name: {}, 保存详情: {} -----'.format(
                                    const.STRATEGY_PROCESS, command_name, tmp_dict))
                return_msg['success'], return_msg['datas'], return_msg['message'] = True, self.strategy_process_sort(
                    result, sort_type, sort_rule) if not whether_command_name else result, '[{}]命令执行成功'.format(
                    self.process_shell)
        except Exception as e:
            logger.error('----- listen_strategy_process, {} -----'.format(traceback.format_exc()))
            return_msg['code'], return_msg['success'], return_msg['datas'], return_msg[
                'message'] = 500, False, tmp_dict, traceback.format_exc()
        finally:
            logger.info('----- listen_strategy_process, 返回详情: {} -----'.format(return_msg))
            return return_msg

    def restart_strategy_process(self, strategy_process):
        ''' 重启strategy进程  '''
        try:
            logger.info('----- restart_strategy_process, 正在重启进程: {} -----'.format(strategy_process))
            print('----- restart_strategy_process, 正在重启进程: {} -----'.format(strategy_process))

            strategy_process_name = strategy_process + '.py'
            dir_path = dirname(dirname(abspath(__file__)))  # 根目录

            if re.search('strategy_new', dir_path):
                port_path = '/data/strategy_new/digital-currency-strategy-trade/{}'.format(strategy_process_name)
            else:
                port_path = '/data/strategy/digital-currency-strategy-trade/{}'.format(strategy_process_name)
            result = subprocess.getstatusoutput('ps aux|grep python3')[1]

            for item in result.split('\n'):
                if re.search(port_path, item):
                    pid = [val for val in item.split(' ') if val][1]
                    logger.info('----- pid：{} -----'.format(pid))
                    subprocess.getstatusoutput('kill -9 {}'.format(pid))
                    logger.info('----- 重启{}进程成功 -----'.format(strategy_process))
                    break
            else:
                logger.info('----- 重启{}进程失败, 未找到对应服务 -----'.format(strategy_process_name))
        except Exception as e:
            logger.error('----- restart_strategy_process, {} -----'.format(traceback.format_exc()))

    def check_strategy_process(self):
        ''' 检查strategy进程 '''
        try:
            for config_process in self.config_monitorprocesses:
                print('----- check_strategy_process, 比对, 正在检测进程: {} -----'.format(config_process))
                count = 0
                while True:
                    strategy_processes_list = self.listen_strategy_process(None, None, True).get('datas', [])  # 已经启动的进程

                    # 判断
                    if config_process not in strategy_processes_list:
                        count += 1
                        logger.info('----- check_strategy_process, 比对, 未检测到进程: {}, 正在重启中, 当前重启次数: {} -----'.format(
                            config_process, count))
                        print('----- check_strategy_process, 比对, 未检测到进程: {}, 正在重启中, 当前重启次数: {} -----'.format(
                            config_process, count))
                        self.restart_strategy_process(config_process)
                    else:  # 小于指定次数 + 恢复, 退出循环
                        whether_recovery = self.email_process_notice_record_dict.get(config_process,
                                                                                     True)  # 上一次恢复记录. 默认上次 True
                        if not whether_recovery:  # 恢复发送一次
                            self.send_email('环境名称: {}, 监控进程: {}, 启动成功!'.format(self.service_name, config_process),
                                            self.recipients, '进程启动恢复提醒')
                            self.email_process_notice_record_dict[config_process] = True  # 更新
                        break

                    if count == self.restart_strategy_process_count:  # 次数满
                        if self.whether_send_email:  # 是否发送邮件
                            whether_recovery = self.email_process_notice_record_dict.get(config_process, None)
                            if whether_recovery == None or whether_recovery == True:
                                logger.info(
                                    '----- 重启{}次失败, 发送邮件, 邮件内容: {}进程启动失败, 请检查... -----'.format(count, config_process))
                                self.send_email(
                                    '环境名称: {}, 监控进程: {}, 启动失败, 请检查! '.format(self.service_name, config_process),
                                    self.recipients, '进程启动失败提醒')

                                self.email_process_notice_record_dict[config_process] = False  # 更新

                        break  # 指定次数, 退出

                    time.sleep(self.restart_strategy_process_interval_time)

        except Exception as e:
            logger.error('----- check_strategy_process, {} -----'.format(traceback.format_exc()))

    def monitor_strategy_process(self):
        ''' 启动监控 '''
        while True:
            logger.info('----- 正在监听strategy进程 -----')
            print('----- 正在监听strategy进程 -----')
            self.check_strategy_process()
            logger.info('----- 进程状态邮件提醒恢复记录: {} -----'.format(self.email_process_notice_record_dict))
            print('----- 进程状态邮件提醒恢复记录: {} -----'.format(self.email_process_notice_record_dict))
            time.sleep(self.monitorprocess_internal_time)

    def process_monitor_process_start(self):
        '''开启进程,监控process'''
        try:
            logger.info('----- 开启进程,监控process -----')
            p = Process(target=self.monitor_strategy_process)
            p.run()
            p.join()
        except Exception as e:
            logger.error('----- process_monitor_process_start, {} -----'.format(traceback.format_exc()))


if __name__ == '__main__':
    obj = MonitorStrategyProcess()
    obj.process_monitor_process_start()
