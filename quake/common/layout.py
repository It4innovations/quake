
class Layout:

    __slots__ = ("size", "offset_r", "offset_c", "block_size_r", "block_size_c")

    def __init__(self, size, offset_r=0, offset_c=0, block_size_r=0, block_size_c=0):
        self.size = size
        self.block_size_r = block_size_r
        self.block_size_c = block_size_c
        self.offset_r = offset_r
        self.offset_c = offset_c

    def iterate(self, rank):
        offset = self.offset_r * rank + self.offset_c
        for i in range(offset, offset + self.block_size_c + self.block_size_r * rank):
            yield i % self.size

    def serialize(self):
        return [self.size, self.offset_r, self.offset_c, self.block_size_r, self.block_size_c]

    @staticmethod
    def deserialize(data):
        return Layout(*data)
