
from quake.common.taskinput import Layout


def test_layouts():
    def check(layout, output_ids, n_parts):
        return [list(layout.iterate(rank, output_ids, n_parts)) for rank in range(4)]

    assert check(Layout(0, 0, 0, 4), [11], 4) == [
        [(11, 0), (11, 1), (11, 2), (11, 3)],
        [(11, 0), (11, 1), (11, 2), (11, 3)],
        [(11, 0), (11, 1), (11, 2), (11, 3)],
        [(11, 0), (11, 1), (11, 2), (11, 3)],
    ]

    assert check(Layout(1, 0, 0, 1), [11], 4) == [
        [(11, 0)], [(11, 1)], [(11, 2)], [(11, 3)],
    ]

    assert check(Layout(1, 0, 0, 1), [11], 2) == [
        [(11, 0)], [(11, 1)], [(11, 0)], [(11, 1)],
    ]

    assert check(Layout(1, 0, 0, 1), [20, 21, 22, 23], 1) == [
        [(20, 0)], [(21, 0)], [(22, 0)], [(23, 0)],
    ]

    assert check(Layout(2, 0, 0, 2), [20, 21, 22, 23], 2) == [
        [(20, 0), (21, 0)], [(22, 0), (23, 0)], [(20, 1), (21, 1)], [(22, 1), (23, 1)],
    ]