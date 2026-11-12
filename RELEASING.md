# Releasing the SDKs

The Python package (`cellaflow`) and the TypeScript package (`@cellaflow/sdk`)
are released together, under one version number.

1. **Open a release PR** that bumps the version in every place it is written,
   all to the same value:
   - `python/pyproject.toml`
   - `typescript/package.json`
   - `typescript/package-lock.json`, both at the top and under `packages[""]`

   and turns `## Unreleased` into `## X.Y.Z` in `python/CHANGELOG.md` and
   `typescript/CHANGELOG.md`. CI's "SDK versions in lockstep" job fails if any
   of the versions disagree.
2. **Merge it.**
3. **Publish a GitHub release** tagged `vX.Y.Z` on `main`, titled
   `Cellaflow SDKs X.Y.Z`. Publishing it runs `publish-sdks.yml`, which uploads
   both packages to PyPI and npm. That cannot be undone, so check the PR's
   version first.

The root README needs no edit: its table shows the published versions through
PyPI and npm badges.
