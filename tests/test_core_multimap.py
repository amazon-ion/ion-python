from amazon.ion.core import Multimap, record
import six

from tests import parametrize


class _P(record('pairs', 'expected_all_values', 'expected_single_value', 'expected_total_len')):
    def __str__(self):
        return '{name}'.format(name=self.pairs)


ALL_DATA = _P(
    pairs=[('a', 1), ('a', 2), ('a', 3), ('a', [4, 5, 6]), ('b', 0), ('c', {'x': 'z', 'r': 's'})],
    expected_all_values=[('a', [1, 2, 3, [4, 5, 6]]), ('b', [0]), ('c', [{'x': 'z', 'r': 's'}])],
    expected_single_value=[('a', [4, 5, 6]), ('b', 0), ('c', {'x': 'z', 'r': 's'})],
    expected_total_len=6
)


def _create_multimap_with_items(pairs):
    m = Multimap()
    for pair in pairs:
        m.add_item(pair[0], pair[1])
    return m


@parametrize(
    ALL_DATA
)
def test_add_item(p):
    m = _create_multimap_with_items(p.pairs)
    for expected in p.expected_all_values:
        assert list([x for x in m.get_all_values(expected[0])]) == expected[1]
    for expected in p.expected_single_value:
        assert m[expected[0]] == expected[1]
    assert p.expected_total_len == len(m)


@parametrize(
    (ALL_DATA, ["a"], 2),
    (ALL_DATA, ["b"], 5),
    (ALL_DATA, ["c"], 5),
    (ALL_DATA, ["a", "b"], 1),
    (ALL_DATA, ["a", "b", "c"], 0)
)
def test_delete_item(item):
    m = _create_multimap_with_items(item[0].pairs)
    p, items_to_remove, len_after_removal = item
    for to_remove in items_to_remove:
        del m[to_remove]
    assert len(m) == item[2]


@parametrize(
    {},
    {"a": 1},
    {"a": 1, "b": 2, "c": [1, 2, {3: 4}]}
)
def test_constructor(d):
    m = Multimap(d)
    for k, v in six.iteritems(d):
        assert m[k] == v
    assert len(m) == len(d)
