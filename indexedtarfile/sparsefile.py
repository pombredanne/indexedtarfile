class SparseFile:
    def __init__(self, size=0, **kwargs):
        if size < 0:
            raise ValueError("`size` cannot be a negative value.")
        self.size = size
        self._pos = 0

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self.size + offset

    def read(self, size=-1):
        if size >= 0:
            return b'\x00' * min(size, (self.size - self._pos))
        else:
            return b'\x00' * (self.size - self._pos)
