## Description

<!-- What does this change do and why? Reference any issue with #NN. -->

## Type of change

<!-- Delete the options that do not apply. -->

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / cleanup
- [ ] Documentation
- [ ] Build / CI / release

## What was tested

<!-- Describe the verification you ran. Note: FIFO-backed tests only run on
Linux; say which host you verified on. -->

- [ ] `./gradlew clean build` passes (or the targeted test run, if a full
      build is not feasible on this host)
- [ ] Targeted tests: <list the `--tests` filters>
- [ ] Manual check: <describe>

## Hard-rule checklist

The project's contributing rules ([CONTRIBUTING.md](../CONTRIBUTING.md))
are enforced by reviewers and gates. Confirm none are violated:

- [ ] No daemon stop endpoint appears in `src/main` (not even in a comment)
- [ ] No `Thread.sleep` in `src/main`
- [ ] Only `spdirect:` identifiers are claimed; source name remains
      `spdirect`
- [ ] No fabricated metadata; lease is acquired at play time, never at load
- [ ] No new runtime dependencies
- [ ] Log lines are sanitized (bearer tokens / credentials / query secrets
      never reach logs)
- [ ] No secrets, `.env`, or `application.yml` committed

## License

By submitting this pull request, you agree that your contribution is licensed
under the project's Apache-2.0 license.
