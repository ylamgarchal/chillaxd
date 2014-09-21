# -*- coding: utf-8 -*-
# Author: Yassine Lamgarchal <lamgarchal.yassine@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import colorlog

import logging
import sys

from server.raft import server


def _setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s :: %(levelname)s :: %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red'
        }
    )
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def main():
    _setup_logging()
    test_local_endpoint = "127.0.0.1:%s" % sys.argv[1]
    test_remote_endpoints = {"127.0.0.1:%s" % sys.argv[2],
                             "127.0.0.1:%s" % sys.argv[3]}
    chillax_server = server.Server(test_local_endpoint, test_remote_endpoints)
    chillax_server.start()

if __name__ == '__main__':
    main()
