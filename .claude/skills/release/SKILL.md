---
name: release-datafusion-ducklake
description: Prepare and publish a new release of datafusion-ducklake to crates.io
disable-model-invocation: true
argument-hint: "[version]"
allowed-tools: Bash(cargo check*), Bash(cargo build*), Bash(git checkout*), Bash(git pull*), Bash(git log*), Bash(git status*), Bash(git add*), Bash(git commit*), Bash(sleep*)

---

# Release datafusion-ducklake

Prepare and publish a new release. The user may provide a version number as `$ARGUMENTS`.

## Step 1: Determine version

- If a version was provided (`$ARGUMENTS`), use it. Strip a leading `v` if present (e.g. `v0.0.7` -> `0.0.7`).
- If no version was provided, read the current version from `Cargo.toml` and suggest the next patch bump. Ask the user to confirm or provide a different version.

## Step 2: Ensure clean, up-to-date main branch

- Confirm we're on `main` and the working tree is clean (`git status`).
- Pull latest: `git pull`.
- If the branch is dirty or not on main, warn the user and stop.

## Step 3: Create release branch

- Create and switch to a new branch: `git checkout -b release/v{VERSION}`

## Step 4: Bump version

- Update `version` in `Cargo.toml` (the `[package]` section, NOT workspace members).
- Run `cargo check` to regenerate `Cargo.lock` with the new version.
- Verify both `Cargo.toml` and `Cargo.lock` reflect the new version.

## Step 5: Update CHANGELOG.md

- Read `CHANGELOG.md` and the git log since the last tag (`git log $(git describe --tags --abbrev=0)..HEAD --oneline`).
- Add a new `## [VERSION] - YYYY-MM-DD` section at the top (below the header), categorized as Added/Changed/Fixed per Keep a Changelog format.
- Add a comparison link at the bottom: `[VERSION]: https://github.com/hotdata-dev/datafusion-ducklake/compare/v{PREV}...v{VERSION}`
- Ask the user to review the changelog entries before committing.

## Step 6: Update README.md if needed

- Check if `README.md` references a version number (e.g. in dependency examples). If so, update to the new version.

## Step 7: Commit and push

- Stage `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, and `README.md` (if changed).
- **IMPORTANT**: Both `Cargo.toml` AND `Cargo.lock` must be committed. Missing the lock file will break the deploy.
- Commit with message: `chore(release): prepare v{VERSION}`
- Push the branch: `git push -u origin release/v{VERSION}`

## Step 8: Create PR

- Create a PR with `gh pr create` targeting `main`.
- Title: `chore(release): prepare v{VERSION}`
- Body should note the version bump and link to the changelog.

## Step 9: Merge PR, tag, and trigger release

After creating the PR, ask the user if they'd like to wait for CI or merge now.

When the user is ready, merge the PR:

```
gh pr merge --squash --delete-branch
```

Then checkout main, pull, tag, and push:

```
git checkout main && git pull
```

Verify the merged changes are present (the new version should be in `Cargo.toml`). If not, stop and warn the user.

```
git tag v{VERSION} && git push origin v{VERSION}
```

## Step 10: Link to pending deployment

After pushing the tag, wait a few seconds, then fetch the workflow run triggered by the tag:

```
gh run list --workflow=release.yml --limit=1 --json databaseId,status,url
```

Extract the run URL and print it for the user:

> Tag `v{VERSION}` pushed. Release workflow started.
>
> **Approve the deployment here:** {RUN_URL}
>
> After approval and deployment:
> - Confirm the new version on [crates.io](https://crates.io/crates/datafusion-ducklake)
> - Confirm docs update on [docs.rs](https://docs.rs/datafusion-ducklake) (may take up to a day due to build queue)