from ztf_archive_downloader.feeder.kafka_io import _base_conf


def test_plaintext_no_sasl_keys():
    conf = _base_conf("broker:9092", "PLAINTEXT", "PLAIN", None, None)
    assert not any(k.startswith("sasl.") for k in conf)


def test_sasl_plaintext_includes_mechanism():
    conf = _base_conf("broker:9092", "SASL_PLAINTEXT", "PLAIN", "user", "pass")
    assert conf["sasl.mechanism"] == "PLAIN"
    assert conf["sasl.username"] == "user"
    assert conf["sasl.password"] == "pass"


def test_sasl_no_credentials():
    conf = _base_conf("broker:9092", "SASL_PLAINTEXT", "PLAIN", None, None)
    assert "sasl.mechanism" in conf
    assert "sasl.username" not in conf
    assert "sasl.password" not in conf


def test_bootstrap_always_set():
    conf = _base_conf("broker:9092", "PLAINTEXT", "PLAIN", None, None)
    assert conf["bootstrap.servers"] == "broker:9092"
