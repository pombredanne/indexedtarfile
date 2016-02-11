import shelve
import os
import tarfile
import mmap
import io

from contextlib import contextmanager

from indexedtarfile.multifile import MultiFile
from indexedtarfile.sparsefile import SparseFile
from indexedtarfile.fileview import FileView

import posix_ipc as ipc


class IndexedTarFile:
    def __init__(self, filename, mode='r'):
        self.closed = False
        self._tar_lock = filename.replace('/', '-')
        self._idx_lock = self.idxfilename(filename).replace('/', '-')
        self._idx_filename = self.idxfilename(filename)
        if mode.startswith('r'):
            self._idx_flag = 'r'
            try:
                self._fd = os.open(filename, flags=os.O_RDONLY)
                self._mm = mmap.mmap(self._fd, 0, prot=mmap.PROT_READ)
                self.tarfile = tarfile.open(
                    fileobj=io.BytesIO(memoryview(self._mm)),
                    mode=mode)
            except Exception as err:
                raise IOError("Cannot load tarfile.") from err
        else:
            self._idx_flag = 'c'
            self.tarfile = tarfile.open(filename, mode=mode)

    @contextmanager
    def _index(self):
        try:
            with shelve.open(self._idx_filename, flag=self._idx_flag) as s:
                yield s
        except Exception as err:
            raise IOError("Cannot open index file.") from err


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.closed:
            return

        try:
            self.tarfile.close()
        except:
            raise
        finally:
            self.closed = True
        try:
            os.close(self._fd)
        except:
            pass

    @classmethod
    def create_index(cls, filename, mode='r'):
        with shelve.open(cls.idxfilename(filename), flag='n') as idx:
            with tarfile.open(filename, mode=mode) as tar:
                for tarinfo in tar.getmembers():
                    idx[tarinfo.name] = tarinfo.offset

    @staticmethod
    def idxfilename(filename):
        return filename + '.idx'

    def gettarinfo(self, filename):
        # Get the offset from the index
        with self._index() as idx:
            offset = idx[filename]

        # Get the tarinfo object from the tar
        self.tarfile.fileobj.seek(offset)

        return tarfile.TarInfo.fromtarfile(self.tarfile)

    def _tarchunk(self, start, size):
        return io.BytesIO(memoryview(self._mm)[start:start+size])

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

                parts.append(self._tarchunk(tarpos, size))

                pos += size
                tarpos += size

            if tarinfo.size > pos:
                parts.append(SparseFile(size=tarinfo.size - pos))

            return MultiFile(files=parts)
        else:
            return self._tarchunk(tarinfo.offset_data, tarinfo.size)

    def addfile(self, filename, arcname=None):
        if arcname is None:
            arcname = filename

        tarinfo = self.tarfile.gettarinfo(filename, arcname)

        if not tarinfo.isreg():
            raise ValueError("Must be a file.")

        try:
            offset = self.tarfile.fileobj.tell()
            with open(filename, 'rb') as f:
                with ipc.Semaphore(self._tar_lock,
                                   flags=ipc.O_CREAT,
                                   initial_value=1):
                    self.tarfile.addfile(tarinfo, f)
        except:
            raise
        else:    
            with ipc.Semaphore(self._idx_lock,
                               flags=ipc.O_CREAT,
                               initial_value=1):
                with self._index() as idx:
                    idx[tarinfo.name] = offset

    def addfilelike(self, fileobj, arcname, size=None, **kwargs):
        tarinfo = tarfile.TarInfo()

        tarinfo.name = arcname
        tarinfo.path = arcname

        # Get the file size if not provided
        if size is None:
            fileobj.seek(0, 2)
            size = fileobj.tell()
            fileobj.seek(0)

        tarinfo.size = size

        # Set additional data
        for key, value in kwargs:
            setattr(tarinfo, key, value)

        try:
            offset = self.tarfile.fileobj.tell()
            with ipc.Semaphore(self._tar_lock,
                               flags=ipc.O_CREAT,
                               initial_value=1):
                self.tarfile.addfile(tarinfo, fileobj)
        except:
            raise
        else:
            # On success update the index
            with self._index() as idx:
                with ipc.Semaphore(self._idx_lock,
                                   flags=ipc.O_CREAT,
                                   initial_value=1):
                    idx[tarinfo.name] = offset
