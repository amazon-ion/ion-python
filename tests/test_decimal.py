# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

from decimal import Decimal
from amazon.ion.simpleion import dumps, loads


# regression test for https://github.com/amzn/ion-python/issues/132
def test_decimal_precision():
    from decimal import localcontext

    with localcontext() as ctx:
        # ensure test executes with the default precision
        # (see https://docs.python.org/3.7/library/decimal.html#decimal.DefaultContext):
        ctx.prec = 28

        # decimal with 29 digits
        decimal = Decimal('1234567890123456789012345678.9')
        assert decimal == loads(dumps(decimal))
        assert decimal == loads(dumps(decimal, binary=False))

        # negative decimal with 29 digits
        decimal = Decimal('-1234567890123456789012345678.9')
        assert decimal == loads(dumps(decimal))
        assert decimal == loads(dumps(decimal, binary=False))
