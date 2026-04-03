from __future__ import annotations

import json
from pathlib import Path

from hungary_ge.pipeline.policy_figures import (
    policy_figure_specs,
    write_figures_manifest,
)


def test_policy_figure_specs_have_required_text() -> None:
    specs = policy_figure_specs("run-x")
    assert len(specs) == 9
    for spec in specs:
        assert spec.filename.endswith(".png")
        assert spec.title.strip() != ""
        assert spec.source.strip() != ""
        assert spec.takeaway.strip() != ""


def test_write_figures_manifest_shape(tmp_path: Path) -> None:
    out = tmp_path / "figures_manifest.json"
    specs = policy_figure_specs("run-y")
    write_figures_manifest(
        out,
        run_id="run-y",
        style_name="memo-light",
        specs=specs,
        focus_counties=["01", "14", "08", "15"],
        n_draws_by_focus={"01": 100, "14": 100},
    )
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "hungary_ge.policy_figures/v1"
    assert payload["run_id"] == "run-y"
    assert payload["style"] == "memo-light"
    assert payload["focus_counties"] == ["01", "14", "08", "15"]
    assert len(payload["figures"]) == 9
    assert payload["figures"][0]["memo_section"] != ""

