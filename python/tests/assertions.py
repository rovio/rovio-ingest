#
# Copyright 2021 Rovio Entertainment Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re
from pandas.testing import assert_frame_equal


def _print_dfs(actual, expected):
    print('actual.show(100):')
    actual.show(100, False)
    print('expected.show(100):')
    expected.show(100, False)


def assert_df(actual, expected, sort=True):
    """
    For more elaborate DFs comparison, e.g. ignoring sorting or less precise.
    Default precision for float comparison is 5 digits.
    """
    __tracebackhide__ = True

    if sort:
        actual = actual.orderBy(actual.columns)
        expected = expected.orderBy(actual.columns)

    try:
        assert_frame_equal(actual.toPandas(), expected.toPandas())
    except AssertionError as e:
        _print_dfs(actual, expected)
        # assert_frame_equal doesn't print the column name, only the index
        #   -> get the index from message with regex & resolve column name
        matcher = re.search(r'iloc\[:, (\d+)\]', e.args[0])
        if matcher:
            iloc = matcher.group(1)
            if iloc is not None:
                raise AssertionError("failed assert on column '{}': {}".format(actual.columns[int(iloc)], e))
        # couldn't extract column name. unexpected.
        raise e
