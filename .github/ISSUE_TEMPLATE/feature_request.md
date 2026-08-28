---
name: Feature request
about: Suggest an idea for lavalink-go-librespot
title: "[feature] "
labels: enhancement
assignees: ""
---

## Is your feature request related to a problem?

Describe the problem you are trying to solve, and who is affected by it.

## Proposed solution

What should the feature do? How would a user interact with it?

## Alternative approaches

What else did you consider, and why does it not work as well?

## Scope check

Before requesting, please confirm whether this is within the project's
stated scope ([README](../../README.md)):

- The plugin controls an **external** go-librespot daemon; it does not
  implement the Spotify protocol itself. Features that require new Spotify
  protocol work belong in the daemon, not this plugin.
- The plugin claims only the `spdirect:` namespace, so LavaSrc keeps search
  and discovery.

If the feature is about playback behavior or daemon integration (seeking,
pause/resume semantics, quarantine policy, metadata), it is in scope. If it
requires bundling, embedding, or modifying go-librespot, it is out of scope.

## Additional context

Links, prior art, or examples of how similar projects handle this.
