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

"""Chillaxd server.

Usage:
  chillaxd [--config-file=<path>]
  chillaxd (-h | --help)
  chillaxd --version

Options:
  --config-file=<path>  The configuration file path
                        [default: /etc/chillaxd/chillaxd.conf].
  -h --help             Show this screen.
  --version             Show version.
"""

import logging
import sys

import colorlog
import docopt
import yaml

from chillaxd.consensus import raft

LOG = logging.getLogger(__name__)

_VERSION = '0.0.1'


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


def _get_arguments(cli_arguments):
    arguments = {}
    config_file_path = cli_arguments["--config-file"]
    if config_file_path:
        try:
            with open(config_file_path, "r") as config_file:
                config = yaml.load(config_file)
                arguments["private_endpoint"] = \
                    config["bind_addresses"]["private"]
                arguments["public_endpoint"] = \
                    config["bind_addresses"]["public"]
                arguments["remote_endpoints"] = config["remote_servers"]
                arguments["leader_heartbeat_interval"] = \
                    config["time_parameters"]["leader_heartbeat_interval"]
                arguments["min_election_timeout"] = \
                    config["time_parameters"]["min_election_timeout"]
                arguments["max_election_timeout"] = \
                    config["time_parameters"]["max_election_timeout"]
        except (OSError, IOError) as e:
            LOG.error("cannot open configuration file '%s'" % config_file_path)
            sys.exit(e.errno)
    return arguments


def main(args=None):

    _setup_logging()
    cli_arguments = docopt.docopt(__doc__,
                                  argv=args or sys.argv[1:],
                                  version="Chillaxd %s" % _VERSION)
    arguments = _get_arguments(cli_arguments)
    chillax_server = raft.Raft(**arguments)
    chillax_server.start()


if __name__ == '__main__':
    main()
