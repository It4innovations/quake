class Layout:
    __slots__ = ("offset_r", "offset_c", "block_size_r", "block_size_c")

    def __init__(self, offset_r=0, offset_c=0, block_size_r=0, block_size_c=0):
        self.block_size_r = block_size_r
        self.block_size_c = block_size_c
        self.offset_r = offset_r
        self.offset_c = offset_c

    def iterate(self, rank, input_ids, n_parts):
        offset = self.offset_r * rank + self.offset_c
        n_ids = len(input_ids)
        for i in range(offset, offset + self.block_size_c + self.block_size_r * rank):
            yield input_ids[i % n_ids], (i // n_ids) % n_parts

    def serialize(self):
        return [self.offset_r, self.offset_c, self.block_size_r, self.block_size_c]

    @staticmethod
    def deserialize(data):
        return Layout(*data)
