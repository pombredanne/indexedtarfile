class FileView:
    def __init__(self, file, offset, size):
        self.file = file
        self.offset = offset
        self.size = size
        self._pos = 0

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = min(offset, self.size)
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self.size + offset
        else:
            raise ValueError("Unknown whence value.")

    def read(self, size=-1):
        basepos = self.file.tell()
        readed = b''
        try:
            self.file.seek(self.offset + self._pos)
            if size < 0:
                available = self.size - self._pos
            else:
                available = min(self.size - self._pos, size)
            readed = self.file.read(available)
        except:
            raise
        else:
            return readed
        finally:
            self._pos += len(readed)
            self.file.seek(basepos)
