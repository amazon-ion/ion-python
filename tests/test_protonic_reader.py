from event_aliases import e_null, e_bool, e_string, e_symbol, _good_list

_GOOD_SCALARS = (
    (b'null', e_null()),

    (b'false', e_bool(False)),
    (b'true', e_bool(True)),
    (b'"spam"', e_string("spam")),
    (b"'eggs'", e_symbol("eggs")),
)
_GOOD_CONTAINERS = (
    (b'[]', _good_list()),
    (b'[null]', _good_list(e_null())),
    (b'[true,false]', _good_list(e_bool(True), e_bool(False)))
)
