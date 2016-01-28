import pytest

from hypothesis import given
import hypothesis.strategies as st


def test_sparsefile():
    try:
        from indexedtarfile import sparsefile
    except ImportError:
        assert False, "Cannot import sparsefile."


def test_SparseFile_class():
    try:
        from indexedtarfile.sparsefile import SparseFile
    except ImportError:
        assert False, "Cannot import SparseFile class."


@given(size=st.integers(min_value=0))
def test_SparseFile_negative_size(size):
    from indexedtarfile.sparsefile import SparseFile

    with pytest.raises(ValueError):
        SparseFile(size=-10)


@given(size=st.integers(min_value=0))
def test_SparseFile_content(size):
    from indexedtarfile.sparsefile import SparseFile

    sf = SparseFile(size=size)
    data = sf.read()

    assert len(data) == size
    assert all(x == 0 for x in bytearray(data))


@given(size=st.integers(min_value=0),
       readsize=st.integers(min_value=0))
def test_SparseFile_readsize(size, readsize):
    from indexedtarfile.sparsefile import SparseFile

    sf = SparseFile(size=size)
    data = sf.read(readsize)

    assert len(data) == min(size, readsize)


@given(size=st.integers(min_value=0))
def test_SparseFile_seek(size):
    from indexedtarfile.sparsefile import SparseFile

    sf = SparseFile(size=size)
    for i in range(0, size):
        sf.seek(i)
        assert len(sf.read()) == size - i


@given(size=st.integers(min_value=0))
def test_SparseFile_seek_whence_2(size):
    from indexedtarfile.sparsefile import SparseFile

    sf = SparseFile(size=size)
    for i in range(0, -(size+1), -1):
        sf.seek(i, 2)
        assert len(sf.read()) == abs(i)
