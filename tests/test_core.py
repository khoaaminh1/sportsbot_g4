from sportsbot.core import normalize, canonical, connect, retrieve, answer


def test_aliases():
    assert normalize('VN Indo football') == 'viet nam indonesia bong da'


def test_unsafe_links():
    assert canonical('javascript:alert(1)') is None
    assert canonical('https://example.com/a#section') == 'https://example.com/a'


def test_empty_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv('SPORTSBOT_DB', str(tmp_path / 'db.sqlite'))
    monkeypatch.delenv('LLM_API_KEY', raising=False)
    assert answer('bóng đá', refresh=False)['mode'] == 'no_evidence'


def test_retrieval_and_no_key_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv('SPORTSBOT_DB', str(tmp_path / 'db.sqlite'))
    monkeypatch.delenv('LLM_API_KEY', raising=False)
    with connect() as db:
        db.execute('INSERT INTO articles VALUES (?,?,?,?,?,?)', ('https://example.com/fixture','Việt Nam gặp Indonesia','Bóng đá Việt Nam','Test fixture',None,'2026-01-01'))
    assert retrieve('VN Indo')[0]['source'] == 'Test fixture'
    result = answer('Vietnam football', refresh=False)
    assert result['mode'] == 'source_excerpts'
    assert result['sources'][0]['published_at'] is None
