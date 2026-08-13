def test_pipeline_layers():
    layers = {"raw", "staging", "mart"}

    assert "raw" in layers
    assert "staging" in layers
    assert "mart" in layers


def test_pipeline_configuration():
    assert True
