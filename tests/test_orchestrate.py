"""Orchestration wiring tests.

These stub out every stage so the sequencing and skip-flag logic is verified
without launching Spark, hitting the network, or touching Delta tables.
"""
import pipeline.orchestrate as orch


def _stub_stages(monkeypatch):
    """Replace all stage side effects with calls recorded onto `order`."""
    order = []
    monkeypatch.setattr(orch.ingest, "ingest", lambda: order.append("ingest"))
    monkeypatch.setattr(orch.bronze, "build_bronze", lambda: order.append("bronze"))
    monkeypatch.setattr(orch.silver, "build_silver", lambda: order.append("silver"))
    monkeypatch.setattr(orch.gold, "build_gold", lambda: order.append("gold"))
    monkeypatch.setattr(orch, "run_quality", lambda layer: order.append(f"quality:{layer}"))
    monkeypatch.setattr(orch.serve, "report", lambda: order.append("serve"))
    return order


def test_full_pipeline_runs_all_stages_in_order(monkeypatch):
    order = _stub_stages(monkeypatch)
    orch.run_pipeline(skip_ingest=False, serve_report=True)
    assert order == [
        "ingest",
        "bronze",
        "silver",
        "quality:silver",
        "gold",
        "quality:gold",
        "serve",
    ]


def test_skip_ingest_omits_fetch(monkeypatch):
    order = _stub_stages(monkeypatch)
    orch.run_pipeline(skip_ingest=True, serve_report=True)
    assert "ingest" not in order
    assert order[0] == "bronze"


def test_no_serve_omits_report(monkeypatch):
    order = _stub_stages(monkeypatch)
    orch.run_pipeline(skip_ingest=True, serve_report=False)
    assert "serve" not in order
    assert order[-1] == "quality:gold"


def test_quality_gate_runs_after_its_layer(monkeypatch):
    order = _stub_stages(monkeypatch)
    orch.run_pipeline(skip_ingest=True, serve_report=False)
    assert order.index("quality:silver") == order.index("silver") + 1
    assert order.index("quality:gold") == order.index("gold") + 1
