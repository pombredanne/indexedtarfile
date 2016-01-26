# Run cpython tarfile test suite


def test_tarfile():
    # Patch test_tarfile to use `indexedtarfile`
    from test import test_tarfile
    # test_tarfile.tarfile = None

    from test import regrtest

    try:
        regrtest.main(['test_tarfile'])
    except SystemExit as exit:
        assert exit.code == 0
