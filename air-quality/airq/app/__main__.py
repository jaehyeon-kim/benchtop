"""`python -m airq.app` runs `airq.app.ui` as a script."""

import runpy

runpy.run_module("airq.app.ui", run_name="__main__", alter_sys=True)
