from collections import OrderedDict, namedtuple
from io import BytesIO


PartialFile = namedtuple('PartialFile', ['fd', 'offset', 'size'])

def getfilesize(fd):
    pos = fd.tell()
    try:
        fd.seek(0, 2)
        return fd.tell()
    finally:
        fd.seek(pos)


class PFDict:
    def __init__(self, *args):
        self.parts = []
        offset = 0
        for fd in args:
            size = getfilesize(fd)
            self.parts.append(PartialFile(fd, offset, size))
            offset += size

    def __getitem__(self, item):
        for p in self.parts:
            if p.offset <= item < (p.offset + p.size):
                return p
        raise KeyError(item)


class MultiFile:
    def __init__(self, files=None, **kwargs):
        if files is None:
            raise ValueError("A list of files is mandatory.")
        self.files = PFDict(*files)
        self._pos = 0
        self.closed = False

    def close(self):
        self.closed = True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=0):
        if whence == 0:
            if offset < 0:
                raise ValueError("Negative seek value")
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            lastfd = self.files.parts[-1]
            lastbyte = lastfd.offset + lastfd.size
            self._pos = lastbyte - offset
        else:
            raise ValueError("Invalid whence value.")

        return self._pos

    def read(self, size=-1):
        out = BytesIO()
        finish = False
        while not finish:
            try:
                cf = self.files[self._pos]
            except KeyError:
                finish = True
            else:
                off = self._pos - cf.offset

                cf.fd.seek(off)
                chunk = cf.fd.read(size)
                chunk_size = len(chunk)

                out.write(chunk)

                self._pos += chunk_size
                size -= chunk_size

                finish = size == 0

        return out.getvalue()
