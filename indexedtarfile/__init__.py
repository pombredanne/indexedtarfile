import shelve
import tarfile

from indexedtarfile.multifile import MultiFile
from indexedtarfile.sparsefile import SparseFile
from indexedtarfile.fileview import FileView


class IndexedTarFile:
    def __init__(self, filename, mode='r'):
        self._idx_filename = self.idxfilename(filename)
        try:
            self._idx = shelve.open(
                self._idx_filename,
                flag='r' if mode == 'r' else 'c')
        except:
            raise IOError("Cannot open index file.")

        self.tarfile = tarfile.open(filename, mode=mode)

    @classmethod
    def create_index(cls, filename):
        idx = shelve.open(cls.idxfilename(filename), flag='n')
        try:
            with tarfile.open(filename, mode='r') as tar:
                for tarinfo in tar.getmembers():
                    idx[tarinfo.name] = tarinfo
        finally:
            idx.close()

    @staticmethod
    def idxfilename(filename):
        return filename + '.idx'

    def readfile(self, filename):
        tarinfo = self._idx[filename]
        sparse = getattr(tarinfo, 'sparse', None)
        if sparse is not None:
            parts = []
            pos = 0
            tarpos = tarinfo.offset_data
            for offset, size in sorted(sparse):
                sparsesize = offset - pos
                if sparsesize > 0:
                    parts.append(SparseFile(size=sparsesize))
                    pos += sparsesize
                parts.append(FileView(self.tarfile.fileobj,
                                      tarpos,
                                      size))
                pos += size
                tarpos += size

            if tarinfo.size > pos:
                parts.append(SparseFile(size=tarinfo.size - pos))

            return MultiFile(files=parts)
        else:
            return FileView(self.tarfile.fileobj,
                            tarinfo.offset_data,
                            tarinfo.size)
