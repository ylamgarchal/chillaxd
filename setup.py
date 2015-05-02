#!/usr/bin/env python
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

import setuptools
import os

import chillaxd


def get_requirements():
    requirements_path = "%s/%s" % (os.path.dirname(os.path.abspath(__file__)),
                                   "requirements.txt")
    with open(requirements_path, "r") as f:
        requirements = f.read()
        return requirements.split("\n")

setuptools.setup(
    name='chillaxd',
    version=chillaxd.__VERSION__,
    packages=setuptools.find_packages(),
    author="Yassine Lamgarchal",
    author_email="lamgarchal.yassine@gmail.com",
    description="Distributed coordination framework based on ZeroMQ and "
                "RAFT consensus algorithm.",
    long_description=open('README.rst').read(),
    install_requires=get_requirements(),
    url="https://github.com/ylamgarchal/chillaxd",
    licence="Apache v2.0",
    include_package_data=True,
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Topic :: System :: Distributed Computing"
    ],
    entry_points={
        "console_scripts": [
            "chillaxd = chillaxd.chillaxd:main",
            "chillax = chillaxd.chillax:main"
        ],
    },
)
