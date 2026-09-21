# benchtop

Small projects, built and tested on a bench. Each one is self-contained, runs from a cold clone, and is small enough to stand up in an afternoon. Bigger work gets its own repository.

## Layout

One directory per project. Each carries its own `README.md` saying what it builds, the versions it was last run against, and the single command that starts it. Nothing at the root is shared, so a directory can be read, run and understood without the others.

```
benchtop/
  <project>/
    README.md      what it builds, versions, how to run, how to tear down
    ...
```

## Conventions

Pin the versions in each project's README rather than at the root, because the tools move faster than the projects do. `odctl` has shipped 18 releases and `dynamic-des` 13, several in the last month, so a single version at the root would be wrong for most directories within weeks.

A project that needs someone else's account says so in its first line and carries a teardown command. Most run locally and cost nothing.

## Adding a project

Create the directory, write its README first, then make it run from a clean checkout. If it cannot be started with one command, it is not finished.
