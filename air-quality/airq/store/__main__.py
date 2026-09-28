"""`python -m airq.store` runs `airq.store.feast_repo` as a script."""

import runpy

runpy.run_module("airq.store.feast_repo", run_name="__main__", alter_sys=True)
