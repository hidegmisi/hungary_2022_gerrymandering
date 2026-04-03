from __future__ import annotations

from hungary_ge.pipeline.policy_figures import select_focus_counties


def _national_payload() -> dict:
    return {
        "partisan": {
            "metrics": {
                "seat_share_a": {
                    "by_county": [
                        {
                            "maz": "01",
                            "weight": 0.16,
                            "focal_value": 0.62,
                            "ensemble_mean": 0.50,
                            "percentile_rank": 92.0,
                        },
                        {
                            "maz": "14",
                            "weight": 0.10,
                            "focal_value": 0.58,
                            "ensemble_mean": 0.53,
                            "percentile_rank": 81.0,
                        },
                        {
                            "maz": "08",
                            "weight": 0.07,
                            "focal_value": 0.61,
                            "ensemble_mean": 0.49,
                            "percentile_rank": 88.0,
                        },
                        {
                            "maz": "15",
                            "weight": 0.06,
                            "focal_value": 0.43,
                            "ensemble_mean": 0.57,
                            "percentile_rank": 12.0,
                        },
                        {
                            "maz": "19",
                            "weight": 0.05,
                            "focal_value": 0.51,
                            "ensemble_mean": 0.50,
                            "percentile_rank": 53.0,
                        },
                    ]
                }
            }
        }
    }


def test_select_focus_counties_fixed_plus_delta_rule() -> None:
    picked = select_focus_counties(_national_payload(), n=4)
    assert picked[:2] == ["01", "14"]
    assert len(picked) == 4
    assert "08" in picked
    assert "15" in picked

