import os
from src.fs_ops import get_unique_path


class TestGetUniquePath:
    def test_no_existing_returns_original(self, tmp_path):
        dest = os.path.join(str(tmp_path), "file.txt")
        result = get_unique_path(dest)
        assert result == dest

    def test_existing_adds_number(self, tmp_path):
        base = str(tmp_path)
        original = os.path.join(base, "file.txt")
        with open(original, "w") as f:
            f.write("test")

        result = get_unique_path(original)
        expected = os.path.join(base, "file-1.txt")
        assert result == expected

    def test_multiple_existing(self, tmp_path):
        base = str(tmp_path)
        for name in ["file.txt", "file-1.txt", "file-2.txt"]:
            with open(os.path.join(base, name), "w") as f:
                f.write("test")

        result = get_unique_path(os.path.join(base, "file.txt"))
        expected = os.path.join(base, "file-3.txt")
        assert result == expected

    def test_no_extension(self, tmp_path):
        base = str(tmp_path)
        original = os.path.join(base, "README")
        with open(original, "w") as f:
            f.write("test")

        result = get_unique_path(original)
        expected = os.path.join(base, "README-1")
        assert result == expected

    def test_multiple_extensions(self, tmp_path):
        base = str(tmp_path)
        original = os.path.join(base, "archive.tar.gz")
        with open(original, "w") as f:
            f.write("test")

        result = get_unique_path(original)
        expected = os.path.join(base, "archive.tar-1.gz")
        assert result == expected
