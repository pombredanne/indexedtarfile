import os
from io import BytesIO

from hypothesis import given
import hypothesis.strategies as st
import pytest


def test_fileview_import():
    try:
        from indexedtarfile import fileview
    except ImportError:
        assert False, "Cannot import `fileview`."


def test_FileView_class():
    try:
        from indexedtarfile.fileview import FileView
    except ImportError:
        assert False, "Cannot import FileView."


@given(size=st.integers(min_value=0),
       offset=st.integers(min_value=0),
       readsize=st.integers(min_value=0))
def test_FileView_read(size, offset, readsize):
    from indexedtarfile.fileview import FileView

    basefile = BytesIO(os.urandom(size))
    basefile.seek(offset)
    buf = basefile.read(readsize)
    basefile.seek(0)
    
    assert FileView(basefile, offset=offset, size=readsize).read() == buf


def test_FileView_multiple_reads():
    from indexedtarfile.fileview import FileView

    basefile = BytesIO(os.urandom(1024))
    fv = FileView(basefile, 512, 512)

    basefile.seek(512)
    for i in range(0, 512, 4):
        assert basefile.read(4) == fv.read(4)

    assert basefile.read(10) == fv.read(10)


def test_FileView_multiple_reads_with_seeks():
    from indexedtarfile.fileview import FileView
    import random

    basefile = BytesIO(os.urandom(1024))
    fv = FileView(basefile, 512, 512)

    basefile.seek(512)
    offsets = list(range(0, 512, 4))
    random.shuffle(offsets)
    for i in offsets:
        fv.seek(i)
        basefile.seek(i+512)
        assert basefile.read(4) == fv.read(4)
