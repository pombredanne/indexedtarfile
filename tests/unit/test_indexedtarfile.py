import os
import tempfile

import pytest


def test_indexedtarfile_import():
    try:
        import indexedtarfile
    except ImportError:
        assert False, "Cannot import `indexedtarfile`."


def test_IndexedTarFile_class():
    try:
        from indexedtarfile import IndexedTarFile
    except ImportError:
        assert False, "Cannot import `IndexedTarFile`."


def test_IndexedTarFile_open_read_without_idx_raises():
    from indexedtarfile import IndexedTarFile

    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(IOError):
            IndexedTarFile(f.name, mode='r')


def test_IndexedTarFile_open_write_without_idx_does_not_raise():
    from indexedtarfile import IndexedTarFile

    try:
        filename = tempfile.mktemp()
        IndexedTarFile(filename, mode='w')
    finally:
        try:
            os.unlink(filename + '.idx')
            os.unlink(filename)
        except:
            pass


def test_IndexedTarFile_open_read_idx_exists_tar_doesnt():
    from indexedtarfile import IndexedTarFile
    import shelve

    try:
        idxfilename = tempfile.mktemp(suffix='.tar.idx')
        s = shelve.open(idxfilename, flag='c')
    
        with pytest.raises(IOError):
            IndexedTarFile(idxfilename[:-4], mode='r')
    finally:
        try:
            s.close()
            os.unlink(idxfilename)
        except:
            pass


def test_IndexedTarFile_create_index():
    from indexedtarfile import __file__ as ROOTFILE
    from indexedtarfile import IndexedTarFile
    from glob import glob
    import tarfile
    import shelve

    attrs = ("name", "mode", "uid", "gid", "size", "mtime",
             "chksum", "type", "linkname", "uname", "gname",
             "devmajor", "devminor", "offset", "offset_data",
             "sparse")

    def members_to_set(members):
        s = set()

        for member in members:
            member_attrs = []
            for attr in attrs:
                member_attrs.append((attr, getattr(member, attr, None)))
            s.add(tuple(member_attrs))

        return s

    try:
        filename = tempfile.mktemp(suffix='.tar')

        # Create the file
        tar = tarfile.open(filename, mode='w')
        tar.add(os.path.dirname(ROOTFILE))
        tar.close()

        # Open the file so the tarinfo members are readed back.
        tar = tarfile.open(filename, mode='r')

        IndexedTarFile.create_index(filename)

        assert glob(filename + '.idx*')

        real_members = tar.getmembers()

        s = shelve.open(filename + '.idx', flag='r')
        index_members = list(s.values())
        s.close()

        real_members_attrs = members_to_set(real_members)
        index_members_attrs = members_to_set(index_members)

        assert real_members_attrs == index_members_attrs

    finally:
        try:
            os.unlink(filename)
            os.unlink(filename + '.idx')
        except:
            pass


def test_IndexedTarFile_readfile():
    import tarfile
    from indexedtarfile import IndexedTarFile

    tarfilename = ""

    try:
        filename = tempfile.mktemp(suffix='.tar')
        randombuf = os.urandom(1024)

        with tarfile.open(filename, 'w') as tar:
            with tempfile.NamedTemporaryFile() as f:
                f.write(randombuf)
                f.file.flush()
                tarfilename = f.name
                tar.add(tarfilename)
        
        IndexedTarFile.create_index(filename)
        itf = IndexedTarFile(filename, mode='r')
        innerfile = itf.readfile(tarfilename[1:])
        
        assert innerfile.read() == randombuf

    finally:
        try:
            os.unlink(filename)
        except:
            pass


def test_IndexedTarFile_readfile_sparse():
    import tarfile

    from indexedtarfile import IndexedTarFile
    FIXTURE = os.path.join(os.path.dirname(__file__), '..', 'fixtures',
                           'sparse.tar')

    assert os.path.exists(FIXTURE)
    IndexedTarFile.create_index(FIXTURE)

    itf = IndexedTarFile(FIXTURE, 'r')
    assert getattr(itf._idx['sparse'], 'sparse', None)
    f = itf.readfile('sparse')

    assert f.read(1024**2) == b'\x00' * 1024**2
    assert f.read(3) == b'1MB'
    assert f.read(1024**2) == b'\x00' * 1024**2
    assert f.read(3) == b'2MB'
    assert f.read(1024**2) == b'\x00' * 1024**2
    assert f.read(1) == b''
