# -*- coding: utf-8 -*-
# Copyright Yassine Lamgarchal <lamgarchal.yassine@gmail.com>
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

from __future__ import absolute_import
import chillaxd
from chillaxd.raft import server
from chillaxd import log

import argparse
import ConfigParser
import logging
import os
import sys


LOG = logging.getLogger(__name__)


def _get_arguments(config_file_path="/etc/chillaxd.conf"):
    arguments = {}
    config = ConfigParser.SafeConfigParser()

    if config_file_path:

        if not os.path.exists(config_file_path):
            LOG.error("cannot open configuration file '%s'" % config_file_path)
            sys.exit(1)

        config.read(config_file_path)
        arguments["private_endpoint"] = config.get("bind_addresses", "private")
        arguments["public_endpoint"] = config.get("bind_addresses", "public")
        arguments["remote_endpoints"] = []
        for remote_server in config.options("remote_servers"):
            endpoint = config.get("remote_servers", remote_server)
            arguments["remote_endpoints"].append(endpoint)

        arguments["leader_heartbeat_interval"] = \
            config.getint("time_parameters", "leader_heartbeat_interval")
        arguments["min_election_timeout"] = \
            config.getint("time_parameters", "min_election_timeout")
        arguments["max_election_timeout"] = \
            config.getint("time_parameters", "max_election_timeout")

    return arguments


def _init_conf():
    parser = argparse.ArgumentParser(description='Chillaxd server.')
    parser.add_argument("--config-file", action="store",
                        help="the configuration file path")
    parser.add_argument("--version", action="store_true",
                        help="show version")
    return parser.parse_args()


def main():

    log.setup_logging()
    conf = _init_conf()
    if conf.version:
        print("%s" % chillaxd.__VERSION__)
    elif conf.config_file:
        arguments = _get_arguments(conf.config_file)
        chillax_server = server.RaftServer(**arguments)
        chillax_server.start()


if __name__ == '__main__':
    main()
