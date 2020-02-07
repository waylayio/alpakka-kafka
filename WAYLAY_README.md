# Waylay Instructions

This branch contains a fork of alpakka-kafka with not-yet-released patches.

## Releasing a new patched version

1. Apply the patch(es) with `git cherry-pick`
2. Update version.sbt. The version number should have a `+<n>` suffix that shows how many patches have been applied (i.e. increment `n` by the number of patches applied in the first step).
3. Run `sbt +publish`, and all artifacts should be published to nexus.waylay.io
