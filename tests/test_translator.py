from translation_service.translator import Translator


def test_simulated_translation_is_repeatable(monkeypatch):
    monkeypatch.setenv("TRANSLATION_PROVIDER", "simulated")
    result = Translator().translate("hello world")
    assert result.text == "[模拟中文翻译] hello world"
    assert result.input_tokens == 2
