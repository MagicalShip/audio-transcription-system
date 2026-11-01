from common.text_merge import merge_overlapping_text


def test_removes_words_repeated_by_audio_overlap():
    assert merge_overlapping_text(["one two three", "two three four"]) == "one two three four"


def test_preserves_non_overlapping_text():
    assert merge_overlapping_text(["first part", "second part"]) == "first part second part"


def test_preserves_failure_gap_marker():
    result = merge_overlapping_text(["before", "[TRANSCRIPTION FAILED 30.0s-60.0s]", "after"])
    assert "[TRANSCRIPTION FAILED 30.0s-60.0s]" in result
