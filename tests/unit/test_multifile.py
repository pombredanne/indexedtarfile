import pytest

from hypothesis import given, assume
import hypothesis.strategies as st


def test_multifile_import():
    try:
        from indexedtarfile import multifile
    except ImportError:
        assert False, "Cannot import multifile module."


def test_MultiFile_class():
    try:
        from indexedtarfile.multifile import MultiFile
    except ImportError:
        assert False, "Cannot import MultiFile class."


def test_multifile_getfilesize():
    from indexedtarfile import multifile
    from io import BytesIO

    a = BytesIO(b'\x00'*10)
    b = BytesIO(b'\x00'*30)

    assert multifile.getfilesize(a) == 10
    assert multifile.getfilesize(b) == 30


def test_PFDict():
    try:
        from indexedtarfile.multifile import PFDict
    except ImportError:
        assert False, "Cannot import PFDict."


def test_PFDict_files():
    from indexedtarfile.multifile import PFDict
    from io import BytesIO

    a = BytesIO(b'\xaa'*2)
    b = BytesIO(b'\xbb'*2)
    c = BytesIO(b'\xcc'*2)

    p = PFDict(a, b, c)

    with pytest.raises(KeyError):
        p[-1]

    assert p[0].fd is a
    assert p[1].fd is a
    assert p[2].fd is b
    assert p[3].fd is b
    assert p[4].fd is c
    assert p[5].fd is c

    with pytest.raises(KeyError):
        p[6]


def test_MultiFile_read_with_limit():
    from indexedtarfile.multifile import MultiFile
    from io import BytesIO

    a = BytesIO(b'\xaa'*2)
    b = BytesIO(b'\xbb'*2)
    c = BytesIO(b'\xcc'*2)

    m = MultiFile(files=[a, b, c])

    assert m.read(6) == b'\xaa\xaa\xbb\xbb\xcc\xcc'


def test_MultiFile_read_all():
    from indexedtarfile.multifile import MultiFile
    from io import BytesIO

    a = BytesIO(b'\xaa'*2)
    b = BytesIO(b'\xbb'*2)
    c = BytesIO(b'\xcc'*2)

    m = MultiFile(files=[a, b, c])

    assert m.read() == b'\xaa\xaa\xbb\xbb\xcc\xcc'


def test_MultiFile_read_multiple():
    from indexedtarfile.multifile import MultiFile
    from io import BytesIO

    a = BytesIO(b'\xaa'*2)
    b = BytesIO(b'\xbb'*2)
    c = BytesIO(b'\xcc'*2)

    m = MultiFile(files=[a, b, c])

    assert m.read(3) == b'\xaa\xaa\xbb'
    assert m.read(3) == b'\xbb\xcc\xcc'
    assert m.read(3) == b''


@given(start=st.integers(min_value=0, max_value=6),
       offset=st.integers(min_value=-6, max_value=6),
       whence=st.integers(min_value=0, max_value=2))
def test_MultiFile_seek(start, offset, whence):
    assume(whence < 2 and offset >= 0)
    from indexedtarfile.multifile import MultiFile
    from io import BytesIO


    a = BytesIO(b'\xaa'*2)
    b = BytesIO(b'\xbb'*2)
    c = BytesIO(b'\xcc'*2)

    m = MultiFile(files=[a, b, c])
    j = BytesIO(b'\xaa\xaa\xbb\xbb\xcc\xcc')

    m.seek(start)
    j.seek(start)

    m.seek(offset, whence)
    j.seek(offset, whence)

    assert m.read() == j.read()
