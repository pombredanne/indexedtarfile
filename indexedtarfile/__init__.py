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
                flag='r' if mode.startswith('r') else 'c')
        except:
            raise IOError("Cannot open index file.")

        self.tarfile = tarfile.open(filename, mode=mode)
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if closed:
            return

        try:
            self.tarfile.close()
        except:
            raise
        finally:
            try:
                self._idx.close()
            except:
                raise
            finally:
                self.closed = True

    @classmethod
    def create_index(cls, filename, mode='r'):
        idx = shelve.open(cls.idxfilename(filename), flag='n')
        try:
            with tarfile.open(filename, mode=mode) as tar:
                for tarinfo in tar.getmembers():
                    idx[tarinfo.name] = tarinfo.offset
        finally:
            idx.close()

    @staticmethod
    def idxfilename(filename):
        return filename + '.idx'

    def gettarinfo(self, filename):
        # Get the offset from the index
        offset = self._idx[filename]

        # Get the tarinfo object from the tar
        self.tarfile.fileobj.seek(offset)

        return tarfile.TarInfo.fromtarfile(self.tarfile)

    def readfile(self, filename):

        tarinfo = self.gettarinfo(filename)

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

    def addfile(self, filename, arcname=None):
        if arcname == None:
            arcname = filename

        tarinfo = self.tarfile.gettarinfo(filename, arcname)

        if not tarinfo.isreg():
            raise ValueError("Must be a file.")

        try:
            offset = self.tarfile.fileobj.tell()
            with open(filename, 'rb') as f:
                self.tarfile.addfile(tarinfo, f)
        except:
            raise
        else:
            self._idx[tarinfo.name] = offset

    def addfilelike(self, fileobj, arcname, **kwargs):
        tarinfo = tarfile.TarInfo()

        tarinfo.name = arcname
        tarinfo.path = arcname

        # Get the file size
        fileobj.seek(0, 2)
        size = fileobj.tell()
        fileobj.seek(0)
        tarinfo.size = size

        # Set additional data
        for key, value in kwargs:
            setattr(tarinfo, key, value)

        try:
            offset = self.tarfile.fileobj.tell()
            self.tarfile.addfile(tarinfo, fileobj)
        except:
            raise
        else:
            # On success update the index
            self._idx[tarinfo.name] = offset
