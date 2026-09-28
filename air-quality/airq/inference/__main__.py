"""`python -m airq.inference` runs `airq.inference.infer` as a script."""

import runpy

runpy.run_module("airq.inference.infer", run_name="__main__", alter_sys=True)
