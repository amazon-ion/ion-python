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

import os
import sys
from subprocess import call, check_output, CalledProcessError, Popen, PIPE
from os.path import isfile, join, abspath, split, isdir

_PYPY = hasattr(sys, 'pypy_translation_info')

_IONC_LOCATION = abspath(join(os.sep, 'Users', 'greggt', 'Documents', 'workspace', 'ion-c', 'build', 'release'))
_IONC_INCLUDES_LOCATIONS = {
    'ionc': abspath(join(os.sep, 'Users', 'greggt', 'Documents', 'workspace', 'ion-c', 'ionc', 'inc')),
    'decNumber': abspath(join(os.sep, 'Users', 'greggt', 'Documents', 'workspace', 'ion-c', 'decNumber'))
}
_USERLIB_LOCATION = abspath(join(os.sep, 'usr', 'local', 'lib'))
_USERINCLUDE_LOCATION = abspath(join(os.sep, 'usr', 'local', 'include'))

_LIB_SUFFIX = '.dylib'
_LIB_PREFIX = 'lib'


def _library_exists(name):
    proc = Popen(['ld', '-l%s' % name], stderr=PIPE, stdout=PIPE)
    stdout, stderr = proc.communicate()
    return (b'library not found' not in stdout and
            b'library not found' not in stderr)


def _link_library(name):
    lib_name = '%s%s%s' % (_LIB_PREFIX, name, _LIB_SUFFIX)
    call(['ln', '-s', join(_IONC_LOCATION, name, lib_name), join(_USERLIB_LOCATION, lib_name)])


def _link_includes(name):
    includes_dir = join(_USERINCLUDE_LOCATION, name)
    if not isdir(includes_dir):
        call(['ln', '-s', _IONC_INCLUDES_LOCATIONS[name], includes_dir])


def _download_ionc():
    # TODO placeholder for downloading the dependencies.
    pass


def _install_ionc():
    if _PYPY:  # This is pointless if running with PyPy, which doesn't support CPython extensions anyway.
        return False

    if not _library_exists('ionc'):
        _download_ionc()
        _link_library('ionc')
    if not _library_exists('decNumber'):
        _link_library('decNumber')
    _link_includes('ionc')
    _link_includes('decNumber')


if __name__ == "__main__":
    _install_ionc()
