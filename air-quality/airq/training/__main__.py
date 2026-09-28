"""`python -m airq.training` runs `airq.training.train` as a script."""

import runpy

runpy.run_module("airq.training.train", run_name="__main__", alter_sys=True)
