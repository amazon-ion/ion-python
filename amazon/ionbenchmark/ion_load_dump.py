# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import amazon.ion.simpleion as ion


class IonLoadDump:
    """
    Wrapper for simpleion API that holds some configuration so that the configuration can be encapsulated here instead
    of having to be plumbed through the whole benchmark code.

    Results of profiling indicate that this adds a trivial amount of overhead, even for small data. If Ion Python
    performance improves by >1000% from June 2023, then this may need to be re-evaluated.
    """
    def __init__(self, binary, c_ext=True):
        self._binary = binary
        self._single_value = False
        # Need an explicit check here because if `None` is passed in as an argument, that is different from no argument,
        # and results in an unexpected behavior.
        self._c_ext = c_ext if c_ext is not None else True

    def loads(self, s):
        ion.c_ext = self._c_ext
        return ion.loads(s, single_value=self._single_value)

    def load(self, fp):
        ion.c_ext = self._c_ext
        return ion.load(fp, single_value=self._single_value)

    def dumps(self, obj):
        ion.c_ext = self._c_ext
        return ion.dumps(obj, binary=self._binary)

    def dump(self, obj, fp):
        ion.c_ext = self._c_ext
        return ion.dump(obj, fp, binary=self._binary)
