"""白盒单元测试：纯函数 parse_size_string 与 match_pattern。

这两个函数是规则引擎的解析地基：大小字符串的错误解析会静默改变
规则命中结果（无效输入返回 0），模式匹配的语义偏差会让文件落到
错误的动作上。纯函数、边界密集，直接单元测试最经济。
"""

from src.utils import match_pattern, parse_size_string


class TestParseSizeString:
    """二进制单位换算（1KB = 1024B）与特殊值。"""

    def test_binary_units(self):
        assert parse_size_string("1KB") == 1024
        assert parse_size_string("1MB") == 1024**2
        assert parse_size_string("1GB") == 1024**3
        assert parse_size_string("1TB") == 1024**4

    def test_decimal_values(self):
        assert parse_size_string("1.5GB") == int(1.5 * 1024**3)
        assert parse_size_string("0.5MB") == int(0.5 * 1024 * 1024)

    def test_case_insensitive(self):
        assert parse_size_string("1kb") == 1024
        assert parse_size_string("10Mb") == 10 * 1024 * 1024

    def test_surrounding_whitespace_stripped(self):
        assert parse_size_string("  1KB  ") == 1024

    def test_minus_one_is_special_value(self):
        """-1 是"匹配所有大小"的哨兵值，不是字面大小。"""
        assert parse_size_string("-1") == -1
        assert parse_size_string(-1) == -1

    def test_zero_means_no_limit(self):
        assert parse_size_string("0") == 0

    def test_plain_number_accepted(self):
        assert parse_size_string(100) == 100

    def test_empty_and_none_fall_back_to_zero(self):
        assert parse_size_string("") == 0
        assert parse_size_string(None) == 0

    def test_invalid_string_falls_back_to_zero(self):
        """无效输入静默降级为 0（= 不限制），不抛异常。"""
        assert parse_size_string("banana") == 0


class TestMatchPattern:
    """单级模式匹配任意深度的文件名；多级模式锚定源目录根。"""

    def test_single_level_pattern_matches_basename_at_any_depth(self):
        assert match_pattern("a/b/c/app.log", "*.log")

    def test_single_level_pattern_does_not_match_directory_part(self):
        assert not match_pattern("logs/app.txt", "*.log")

    def test_multi_level_pattern_matches_exact_relative_path(self):
        assert match_pattern("alpha/beta/charlie.txt", "alpha/beta/charlie.txt")

    def test_multi_level_pattern_anchored_at_root(self):
        assert not match_pattern("other/logs/app.log", "logs/*.log")

    def test_star_crosses_directory_separator(self):
        """fnmatch 的 * 不感知 /——多级模式中的 * 可跨越目录层级。"""
        assert match_pattern("logs/a/b.log", "logs/*.log")

    def test_question_mark_single_char(self):
        assert match_pattern("app.log", "app.???")
        assert not match_pattern("app.html", "app.???")

    def test_case_insensitive_both_sides(self):
        assert match_pattern("APP.LOG", "*.log")
        assert match_pattern("app.log", "*.LOG")

    def test_backslash_normalized_in_name_and_pattern(self):
        assert match_pattern("logs\\app.log", "logs/*.log")
        assert match_pattern("logs/app.log", "logs\\*.log")
