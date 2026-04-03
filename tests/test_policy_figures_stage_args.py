from __future__ import annotations

from hungary_ge.pipeline.cli import build_parser
from hungary_ge.pipeline.policy_figures import style_preset
from hungary_ge.pipeline.stages.core import STAGE_CHOICES


def test_stage_choices_include_policy_figures() -> None:
    assert "policy_figures" in STAGE_CHOICES


def test_parser_accepts_policy_figures_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "county",
            "--run-id",
            "r1",
            "--only",
            "policy_figures",
            "--policy-figures-style",
            "memo-print",
            "--policy-figures-skip-draw-level",
            "--policy-figures-outdir",
            "out/figs",
        ]
    )
    assert args.only == ["policy_figures"]
    assert args.policy_figures_style == "memo-print"
    assert args.policy_figures_skip_draw_level is True


def test_style_preset_palette_keys() -> None:
    preset = style_preset("memo-light")
    for key in ("focal", "ensemble", "interval", "warning", "accent", "text", "grid"):
        assert key in preset.colors

