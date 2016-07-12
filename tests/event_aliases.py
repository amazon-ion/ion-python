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

from functools import partial

from amazon.ion.core import IonEvent, IonEventType, IonType, \
                            ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, \
                            ION_VERSION_MARKER_EVENT
from amazon.ion.reader import NEXT_EVENT, SKIP_EVENT, read_data_event


e_scalar = partial(IonEvent, IonEventType.SCALAR)
e_null = partial(e_scalar, IonType.NULL)
e_bool = partial(e_scalar, IonType.BOOL)
e_int = partial(e_scalar, IonType.INT)
e_float = partial(e_scalar, IonType.FLOAT)
e_decimal = partial(e_scalar, IonType.DECIMAL)
e_timestamp = partial(e_scalar, IonType.TIMESTAMP)
e_symbol = partial(e_scalar, IonType.SYMBOL)
e_string = partial(e_scalar, IonType.STRING)
e_clob = partial(e_scalar, IonType.CLOB)
e_blob = partial(e_scalar, IonType.BLOB)

e_null_list = partial(e_scalar, IonType.LIST, None)
e_null_sexp = partial(e_scalar, IonType.SEXP, None)
e_null_struct = partial(e_scalar, IonType.STRUCT, None)

e_start = partial(IonEvent, IonEventType.CONTAINER_START)
e_start_list = partial(e_start, IonType.LIST)
e_start_sexp = partial(e_start, IonType.SEXP)
e_start_struct = partial(e_start, IonType.STRUCT)

e_end = partial(IonEvent, IonEventType.CONTAINER_END)
e_end_list = partial(e_end, IonType.LIST)
e_end_sexp = partial(e_end, IonType.SEXP)
e_end_struct = partial(e_end, IonType.STRUCT)

NEXT = NEXT_EVENT
SKIP = SKIP_EVENT
e_read = read_data_event

INC = ION_STREAM_INCOMPLETE_EVENT
END = ION_STREAM_END_EVENT
IVM = ION_VERSION_MARKER_EVENT
