# Governance

freshDB is currently a **single-maintainer project**.

## Maintainers

| Name | GitHub | Role |
|---|---|---|
| Raman Dwivedi | [@raman20](https://github.com/raman20) | creator, lead maintainer |

## How decisions are made

- Day-to-day decisions (reviews, merges, releases) are made by the maintainers.
- Larger changes, such as a new lens, an on-storage format change, or a new
  public API, start as a GitHub issue or discussion. We write the design down
  and give the community time to comment before it is merged.
- [docs/design-principles.md](docs/design-principles.md) is the tie-breaker.
  Changing a principle or a "refuse to build" item needs a written proposal
  and a maintainer decision recorded in the issue.

## Becoming a maintainer

Contributors with a sustained record of high-quality contributions and reviews
may be invited to become maintainers. As the project grows, this document will
move to a more formal model with several maintainers and lazy consensus.

## Releases

Releases follow semantic versioning. Before 1.0, minor versions may contain
breaking changes, including to the on-storage format, and those changes are
always called out in [CHANGELOG.md](CHANGELOG.md).
