# benchtop

Small projects, built and tested on a bench. Each one is self-contained, runs from a cold clone, and takes about a day to build.

## Layout

One directory per project. Each carries its own `README.md` saying what it builds and how to run it, and can be read, run and understood without the others. Only the code checks are shared, at the root.

```
benchtop/
  <project>/
    README.md      what it builds, how to run it
    ...
```

## Projects

- [air-quality](air-quality/README.md): forecasts daily PM2.5, a measure of air pollution, for the next seven days from weather forecasts, with a monitoring dashboard and a chat assistant.

## Checks

The root holds the checks every project shares:
- `.pre-commit-config.yaml`: file checks, [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and mypy for types;
- `ruff.toml`: tells ruff where each project's packages are;
- `.github/workflows/pipeline.yml`: runs the pre-commit checks and each project's unit tests on GitHub on every push to `main`.

To run the checks before each commit, or on every file:

```bash
uvx pre-commit install
uvx pre-commit run --all-files
```

## Licence

MIT. See [LICENSE](LICENSE).
