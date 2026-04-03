from __future__ import annotations

import pytest

from hungary_ge.pipeline.policy_figures import (
    EFFICIENCY_GAP_SIGN_NOTE,
    PERCENTILE_HEATMAP_SUBTITLE,
    _efficiency_gap_plot_frame,
)


def test_efficiency_gap_direction_text_is_not_contradictory() -> None:
    note = EFFICIENCY_GAP_SIGN_NOTE.lower()
    assert "wasted bloc a" in note
    assert "wasted bloc b" in note
    assert "two-party turnout" in note
    assert "net excess waste for bloc a" in note
    assert "depends on each metric's sign convention" in PERCENTILE_HEATMAP_SUBTITLE
    assert "favor of bloc A" not in PERCENTILE_HEATMAP_SUBTITLE


def test_efficiency_gap_plot_frame_uses_county_json_values() -> None:
    national_report = {
        "partisan": {
            "metrics": {
                "efficiency_gap": {
                    "by_county": [
                        {"maz": "01", "weight": 0.6, "percentile_rank": 80.0},
                        {"maz": "02", "weight": 0.4, "percentile_rank": 20.0},
                    ]
                }
            }
        }
    }
    county_partisan = {
        "01": {
            "metrics": {
                "efficiency_gap": {
                    "focal_value": 0.12,
                    "ensemble_p05": -0.10,
                    "ensemble_p95": 0.22,
                },
                "vote_share_a": {"focal_value": 0.55},
            }
        },
        "02": {
            "metrics": {
                "efficiency_gap": {
                    "focal_value": -0.08,
                    "ensemble_p05": -0.20,
                    "ensemble_p95": 0.05,
                },
                "vote_share_a": {"focal_value": 0.45},
            }
        },
    }

    df = _efficiency_gap_plot_frame(county_partisan, national_report)
    assert df["maz"].tolist() == ["01", "02"]

    row01 = df.loc[df["maz"] == "01"].iloc[0]
    assert row01["focal_value"] == pytest.approx(0.12)
    assert row01["ensemble_p05"] == pytest.approx(-0.10)
    assert row01["ensemble_p95"] == pytest.approx(0.22)
    assert row01["vote_share_a"] == pytest.approx(0.55)
    assert row01["weight"] == pytest.approx(0.6)

    row02 = df.loc[df["maz"] == "02"].iloc[0]
    assert row02["focal_value"] == pytest.approx(-0.08)
    assert row02["ensemble_p05"] == pytest.approx(-0.20)
    assert row02["ensemble_p95"] == pytest.approx(0.05)
    assert row02["vote_share_a"] == pytest.approx(0.45)
    assert row02["weight"] == pytest.approx(0.4)
