from src.utils import parse_size_string, match_pattern


class TestParseSizeString:
    def test_empty_string(self):
        assert parse_size_string("") == 0

    def test_none(self):
        assert parse_size_string(None) == 0

    def test_zero(self):
        assert parse_size_string("0") == 0

    def test_minus_one(self):
        assert parse_size_string("-1") == -1

    def test_minus_one_int(self):
        assert parse_size_string(-1) == -1

    def test_kilobytes(self):
        assert parse_size_string("1KB") == 1024
        assert parse_size_string("10KB") == 10240

    def test_megabytes(self):
        assert parse_size_string("1MB") == 1024 * 1024
        assert parse_size_string("5MB") == 5 * 1024 * 1024

    def test_gigabytes(self):
        assert parse_size_string("1GB") == 1024**3
        assert parse_size_string("2GB") == 2 * 1024**3

    def test_terabytes(self):
        assert parse_size_string("1TB") == 1024**4

    def test_fractional(self):
        assert parse_size_string("1.5GB") == int(1.5 * 1024**3)
        assert parse_size_string("0.5MB") == int(0.5 * 1024 * 1024)

    def test_case_insensitive(self):
        assert parse_size_string("1kb") == 1024
        assert parse_size_string("1mb") == 1024 * 1024
        assert parse_size_string("1gb") == 1024**3

    def test_with_spaces(self):
        assert parse_size_string("  1GB  ") == 1024**3

    def test_invalid_format_returns_zero(self):
        assert parse_size_string("not_a_size") == 0
        assert parse_size_string("abc123") == 0


class TestMatchPattern:
    def test_exact_match(self):
        assert match_pattern("file.txt", "file.txt")

    def test_extension_wildcard(self):
        assert match_pattern("app.log", "*.log")
        assert match_pattern("error.log", "*.log")
        assert not match_pattern("app.txt", "*.log")

    def test_name_wildcard(self):
        assert match_pattern("file.txt", "file.*")
        assert not match_pattern("other.txt", "file.*")

    def test_question_mark(self):
        assert match_pattern("app.log", "app.???")
        assert not match_pattern("app.logs", "app.???")

    def test_case_insensitive(self):
        assert match_pattern("FILE.TXT", "file.txt")
        assert match_pattern("File.Log", "*.LOG")

    def test_subdirectory_pattern(self):
        assert match_pattern("subdir/file.txt", "subdir/*.txt")
        assert match_pattern("subdir/file.txt", "subdir/file.txt")
        assert not match_pattern("other/file.txt", "subdir/*.txt")

    def test_pattern_without_slash_matches_basename_only(self):
        assert match_pattern("deep/nested/path/file.log", "*.log")

    def test_windows_backslash(self):
        assert match_pattern("subdir\\file.txt", "subdir/*.txt")
        assert match_pattern("deep\\nested\\app.log", "*.log")

    def test_nested_wildcard(self):
        assert match_pattern("a/b/c/file.txt", "a/*/c/*.txt")

    def test_complex_pattern(self):
        assert match_pattern("backup_2024.zip", "backup_????.zip")
        assert not match_pattern("backup_20245.zip", "backup_????.zip")
