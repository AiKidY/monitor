#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import zmq
import time
import traceback
from utils.config import config
from strategy_order.utils.logger_cfg import monitor_log

_logger = monitor_log


class ListenPort:
    """监听端口"""

    def __init__(self):
        self.ping_port_array = [config.zmq_rep_port, config.zmq_assets_port, config.zmq_positions_port]

    def ping_port(self, port: int) -> bool:
        """ping 端口"""
        status, poller, socket = False, None, None
        try:
            _logger.info('----- ping 端口{} -----'.format(port))
            if port not in self.ping_port_array:
                return False

            if port in [config.zmq_rep_port]:  # 11099
                socket_url = 'tcp://localhost:{}'.format(port)
                socket = zmq.Context().socket(zmq.REQ)

                socket.connect(socket_url)
                poller = zmq.Poller()
                poller.register(socket, zmq.POLLIN)
                socket.send_multipart(
                    [bytes(json.dumps({'type': 'ping pong', 'timestamp': int(time.time() * 10 ** 6)}), "UTF-8"), ])

            elif port in [config.zmq_assets_port, config.zmq_positions_port]:  # 资产和持仓
                socket_url = config.zmq_host.format(port)
                socket = zmq.Context().socket(zmq.SUB)

                socket.connect(socket_url)
                socket.subscribe(b"")
                poller = zmq.Poller()
                poller.register(socket, zmq.POLLIN)

            # 超时判断
            if poller.poll(3000):
                message = socket.recv_multipart()
                print(f'----- ping {port} success, message: {message} -----')
                _logger.info(f'----- ping {port} success, message: {message} -----')
                status = True
            else:
                print('----- ping {} port fail, no response. -----'.format(port))
                _logger.info('----- ping {} port fail, no response. -----'.format(port))
        except Exception as e:
            msg = traceback.format_exc()
            print('----- ping port {}, {} -----'.format(port, msg))
            _logger.error('----- ping port: {}, {} -----'.format(port, msg))
        finally:
            if port in [config.zmq_rep_port]:
                socket.setsockopt(zmq.LINGER, 0)  # 值0指定无延迟时间。在调用zmq_disconnect（）或zmq_close（）之后，应立即丢弃待处理的消息。
                socket.close()
                poller.unregister(socket)  # 删除监听
            elif port in [config.zmq_assets_port, config.zmq_positions_port]:
                del poller

            return status


if __name__ == "__main__":
    obj = ListenPort()
    print(obj.ping_port(11099))