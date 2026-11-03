"""Unit tests for the pure-Python text helpers used by the Spark Silver job (Task 6).

The helpers live next to the Spark jobs (pipelines/spark/jobs/textnorm.py) because they are
shipped to the executors with the job; they import nothing from Spark, so they run here.
"""

from __future__ import annotations

import sys
import unicodedata
from pathlib import Path

import pytest

JOBS_DIR = Path(__file__).resolve().parents[2] / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import textnorm  # noqa: E402


# ------------------------------------------------------------------ normalize_text ----
def test_normalize_composes_unicode_to_nfc():
    decomposed = unicodedata.normalize("NFD", "Đội tuyển Việt Nam")
    assert decomposed != "Đội tuyển Việt Nam"  # the test input really is decomposed
    assert textnorm.normalize_text(decomposed) == "Đội tuyển Việt Nam"


def test_normalize_collapses_whitespace_and_strips():
    assert textnorm.normalize_text("  Messi \n\n ghi\tbàn  ") == "Messi ghi bàn"


def test_normalize_keeps_case_and_punctuation():
    # Silver keeps readable text; only shingling lower-cases and drops punctuation.
    assert textnorm.normalize_text("U23 Việt Nam: 2-1!") == "U23 Việt Nam: 2-1!"


@pytest.mark.parametrize("value", [None, "", "   \n\t "])
def test_normalize_empty_values_become_empty_string(value):
    assert textnorm.normalize_text(value) == ""


# ------------------------------------------------------------------------- tokens ----
def test_tokens_are_lower_case_and_keep_accents():
    assert textnorm.tokens("Đội Tuyển VIỆT NAM thắng!") == ["đội", "tuyển", "việt", "nam", "thắng"]


def test_tokens_drop_punctuation_but_keep_numbers():
    assert textnorm.tokens("Tỷ số 2-1, phút 90+3.") == ["tỷ", "số", "2", "1", "phút", "90", "3"]


def test_tokens_are_identical_for_nfc_and_nfd_input():
    text = "Hoàng Đức ghi bàn ở phút cuối"
    assert textnorm.tokens(unicodedata.normalize("NFD", text)) == textnorm.tokens(text)


# ---------------------------------------------------------------------- shingles ----
def test_word_shingles_are_sliding_windows_of_three_words():
    assert textnorm.word_shingles("Việt Nam thắng Thái Lan") == [
        "nam thắng thái",
        "thắng thái lan",
        "việt nam thắng",
    ]


def test_word_shingles_are_unique_and_sorted():
    # "a b c a b c" has windows abc, bca, cab, abc -> 3 distinct shingles.
    assert textnorm.word_shingles("a b c a b c") == ["a b c", "b c a", "c a b"]


def test_short_text_gives_one_shingle_with_all_words():
    assert textnorm.word_shingles("Messi") == ["messi"]
    assert textnorm.word_shingles("Messi ghi") == ["messi ghi"]


@pytest.mark.parametrize("value", [None, "", "  ", "!!! ..."])
def test_text_without_words_gives_no_shingles(value):
    assert textnorm.word_shingles(value) == []


def test_shingles_preserve_accents():
    # "bóng đá" and "bong da" are different words: accents are kept on purpose.
    assert textnorm.word_shingles("bóng đá Việt") != textnorm.word_shingles("bong da Viet")


def test_shingle_size_is_configurable():
    assert textnorm.word_shingles("a b c d", n=2) == ["a b", "b c", "c d"]


def test_shingle_size_must_be_positive():
    with pytest.raises(ValueError):
        textnorm.word_shingles("a b c", n=0)


# ----------------------------------------------------------------------- jaccard ----
def test_jaccard_identical_and_disjoint_sets():
    assert textnorm.jaccard(["a", "b"], ["b", "a"]) == 1.0
    assert textnorm.jaccard(["a"], ["b"]) == 0.0


def test_jaccard_partial_overlap():
    # |{b, c}| / |{a, b, c, d}| = 2 / 4
    assert textnorm.jaccard(["a", "b", "c"], ["b", "c", "d"]) == 0.5


def test_jaccard_of_two_empty_sets_is_zero():
    # Two articles without words are not evidence of the same story.
    assert textnorm.jaccard([], []) == 0.0


def test_near_duplicate_rewrite_scores_above_threshold():
    original = (
        "Đội tuyển Việt Nam đánh bại Thái Lan với tỷ số 2-1 trong trận chung kết "
        "diễn ra tối qua trên sân Mỹ Đình trước sự chứng kiến của bốn mươi nghìn khán giả"
    )
    repost = original + " theo phóng viên"  # same story, three extra words at the end
    score = textnorm.jaccard(textnorm.word_shingles(original), textnorm.word_shingles(repost))
    assert score >= textnorm.NEAR_DUPLICATE_JACCARD


def test_fixed_rules_match_the_handoff():
    # docs/handoff/VNP.md, Task 6: fixed before any run, never tuned after seeing results.
    assert textnorm.SHINGLE_SIZE == 3
    assert textnorm.NEAR_DUPLICATE_JACCARD == 0.8
    assert textnorm.MINHASH_NUM_HASH_TABLES == 5
