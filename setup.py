# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from setuptools import setup, find_packages, Extension

C_EXT = True


def run_setup():
    if C_EXT:
        print('C extension is enabled!')
        kw = dict(
            ext_modules=[
                Extension(
                    'amazon.ion.ionc',
                    sources=['amazon/ion/ioncmodule.c'],
                    include_dirs=['amazon/ion/ion-c-build/include',
                                  'amazon/ion/ion-c-build/include/ionc',
                                  'amazon/ion/ion-c-build/include/decNumber'],
                    libraries=['ionc', 'decNumber'],
                    library_dirs=['amazon/ion/ion-c-build/lib'],
                    extra_link_args=['-Wl,-rpath,%s' % '$ORIGIN/ion-c-build/lib',  # LINUX
                                     '-Wl,-rpath,%s' % '@loader_path/ion-c-build/lib'  # MAC
                                     ],
                ),
            ],
        )
    else:
        print('Using pure python implementation.')
        kw = dict()


    setup(
        name='amazon.ion',
        version='0.7.98',
        description='A Python implementation of Amazon Ion.',
        url='http://github.com/amzn/ion-python',
        author='Amazon Ion Team',
        author_email='ion-team@amazon.com',
        license='Apache License 2.0',


        packages=find_packages(exclude=['tests*']),
        include_package_data=True,
        namespace_packages=['amazon'],

        install_requires=[
            'six',
            'jsonconversion'
        ],

        setup_requires=[
            'pytest-runner',
        ],

        tests_require=[
            'pytest',
        ],
        **kw
    )


run_setup()
