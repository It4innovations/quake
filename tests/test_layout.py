
from quake.common.taskinput import Layout


def test_layouts():
    def check(layout):
        return [list(layout.iterate(rank)) for rank in range(4)]

    assert check(Layout(4, 0, 0, 0, 4)) == [
        [0, 1, 2, 3],
        [0, 1, 2, 3],
        [0, 1, 2, 3],
        [0, 1, 2, 3],
    ]

    assert check(Layout(4, 1, 0, 0, 1)) == [
        [0], [1], [2], [3],
    ]

    assert check(Layout(2, 1, 0, 0, 1)) == [
        [0], [1], [0], [1],
    ]