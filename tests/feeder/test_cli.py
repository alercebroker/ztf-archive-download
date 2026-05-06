import pytest
import typer

from ztf_archive_downloader.feeder.cli import _parse_pipeline_gates


def test_single_gate():
    assert _parse_pipeline_gates(["group:topic"]) == [("group", "topic")]


def test_multiple_gates():
    assert _parse_pipeline_gates(["a:x", "b:y"]) == [("a", "x"), ("b", "y")]


def test_colon_in_topic():
    assert _parse_pipeline_gates(["group:topic:extra"]) == [("group", "topic:extra")]


def test_missing_colon():
    with pytest.raises(typer.BadParameter):
        _parse_pipeline_gates(["grouponly"])


def test_empty_group():
    with pytest.raises(typer.BadParameter):
        _parse_pipeline_gates([":topic"])


def test_empty_topic():
    with pytest.raises(typer.BadParameter):
        _parse_pipeline_gates(["group:"])


def test_empty_list():
    with pytest.raises(typer.BadParameter):
        _parse_pipeline_gates([])
