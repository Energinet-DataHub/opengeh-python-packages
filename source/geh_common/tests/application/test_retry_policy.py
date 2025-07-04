import time

import pytest

from geh_common.application.retry_policy import retry_policy


def test_retry_policy_success():
    calls = []

    @retry_policy(delay=1, retries=3)
    def func():
        calls.append(1)
        return "ok"

    assert func() == "ok"
    assert len(calls) == 1


def test_retry_policy_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch):
    calls = []

    @retry_policy(delay=1, retries=3)
    def func():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("fail")
        return "success"

    # Patch time.sleep to avoid actual delay
    monkeypatch.setattr(time, "sleep", lambda x: None)
    assert func() == "success"
    assert len(calls) == 3


def test_retry_policy_exceeds_retries(monkeypatch: pytest.MonkeyPatch):
    calls = []

    @retry_policy(delay=1, retries=2)
    def func():
        calls.append(1)
        raise RuntimeError("fail always")

    monkeypatch.setattr(time, "sleep", lambda x: None)
    with pytest.raises(RuntimeError) as excinfo:
        func()
    assert "Giving up after" in str(excinfo.value)
    assert len(calls) == 3


def test_retry_policy_delay_doubles(monkeypatch: pytest.MonkeyPatch):
    delays = []

    def fake_sleep(secs):
        delays.append(secs)

    monkeypatch.setattr(time, "sleep", fake_sleep)

    @retry_policy(delay=1, retries=3)
    def func():
        raise Exception("fail")

    with pytest.raises(RuntimeError):
        func()
    assert delays == [1, 2, 4]


def test_retry_policy_passes_args(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)

    @retry_policy(delay=1, retries=2)
    def func(a, b=2):
        return a + b

    assert func(3, b=4) == 7
