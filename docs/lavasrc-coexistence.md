# Using this plugin with LavaSrc

[LavaSrc](https://github.com/topi314/LavaSrc) provides Spotify search and
discovery for Lavalink. This plugin (`spotify`) provides Spotify playback
through a go-librespot daemon. They are designed to run side by side: LavaSrc
finds tracks, `spotify` plays them.

## Who claims what

| Input | LavaSrc | spotify |
| --- | --- | --- |
| `open.spotify.com/track/...` URLs | claimed | not claimed |
| `open.spotify.com/album/...`, `/playlist/...`, `/artist/...` | claimed | not claimed |
| LavaSrc search prefixes (`spsearch:`, `sprec:`, `spprev:`) | claimed | not claimed |
| `spotify:track:<id>` URIs | **not claimed** | not claimed |
| `spdirect:<22-char-base62-id>` | not claimed | claimed |
| `spdirect:spotify:track:<id>` | not claimed | claimed |

Neither source claims `spotify:track:` URIs. If a client sends one, Lavalink
falls through every source and returns nothing.

`spotify` returns `null` for ordinary Spotify URIs and URLs. It recognizes
track-shaped ones only to build a diagnostic hint for the equivalent
`spdirect:<id>` form; it never claims them. This keeps coexistence independent
of source registration order.

## The workflow

Load or search through LavaSrc, read the returned track's
`info.identifier`, and load that identifier as `spdirect:<id>`.

1. **Search** with LavaSrc:

   ```
   GET /v4/loadtracks?identifier=spsearch:Never%20Gonna%20Give%20You%20Up
   ```

   LavaSrc returns `AudioTrack` objects. Each carries a Spotify track id in
   its standard `info.identifier` field (LavaSrc 4.8.x exposes the id here;
   this is the value to hand on).

2. **Take the identifier** from the result:

   ```
   track.info.identifier   ->   "4uLU6hMCjMI75M1A2tKUQC"
   ```

3. **Play it through spotify**:

   ```
   GET /v4/loadtracks?identifier=spdirect:4uLU6hMCjMI75M1A2tKUQC
   ```

   Or, if you have the full URI handy, the equivalent
   `spdirect:spotify:track:4uLU6hMCjMI75M1A2tKUQC`.

4. **Queue** the returned spdirect track like any other track. Replacement,
   seeking, pause, and resume are handled by the plugin's coordinator.

### Playlist / album results

LavaSrc returns playlists and albums as `AudioPlaylist` objects whose tracks
are the individual `AudioTrack`s. Apply the same transformation per track:
read each track's `info.identifier` and load `spdirect:<id>`.

### Client-side helper

If your client wants a single helper, something like:

```
function toSpdirect(track) {
  return {
    ...track,
    identifier: "spdirect:" + track.info.identifier
  };
}
```

is enough, as long as the LavaSrc track's `info.identifier` is the bare
22-character Spotify track id. Do not attempt to derive the id from the
`uri` field of a LavaSrc result; use `info.identifier`.

## Why coexistence stays deterministic

The two plugins must not collide on claim behavior:

- This plugin's source is named `spotify` and claims only the `spdirect:`
  namespace; it never claims Spotify URIs or URLs, so it cannot shadow
  LavaSrc's search behavior.
- Claim-based routing also makes client routing explicit: search results and
  playable tracks are different objects, and there is no ambiguity about which
  plugin produced a track.

## Migration

Moving from LavaSrc-only (or from another Spotify source) to `spotify`:

1. Keep LavaSrc installed for search.
2. Add the `spotify` source per the [README](../README.md) install steps.
3. Point clients at `spdirect:<id>` for playback while keeping search on
   LavaSrc.
4. Existing playlists queued as LavaSrc tracks can stay as-is; only new loads
   need the `spdirect:` prefix. Mixing both in one queue works: the bridge
   routes LavaSrc and other sources' tracks untouched.
