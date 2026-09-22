# benchtop

Small projects, built and tested on a bench. Each one is self-contained, runs from a cold clone, and takes about a day to build.

## Layout

One directory per project. Each carries its own `README.md` saying what it builds, the versions it was last run against, and the single command that starts it. Nothing at the root is shared, so a directory can be read, run and understood without the others.

```
benchtop/
  <project>/
    README.md      what it builds, versions, how to run, how to tear down
    ...
```

## Licence

MIT. See [LICENSE](LICENSE).

A project may depend on tools under other licences. Each project's README names its own.
