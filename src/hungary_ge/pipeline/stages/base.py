"""Stage module contract (implemented by ``*_stage.py`` files).

Each stage module MUST expose:

* ``NAME: str`` — key used with ``--only`` and in :data:`hungary_ge.pipeline.stages.STAGE_RUNNERS`.
* ``add_arguments(parser)`` — register CLI flags owned by this stage (use a no-op for stages
  that only consume core flags, e.g. ``votes``).
* ``run(ctx)`` — execute the stage; return ``0`` on success, non-zero on failure.

Orchestration: :mod:`hungary_ge.pipeline.runner` dispatches ``run`` in ``--only`` order.
Argument registration: :func:`hungary_ge.pipeline.stages.register_stage_arguments`.
"""
