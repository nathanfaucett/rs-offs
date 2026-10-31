# 03: Browser conformance harness and browser guide

**What to build:** The same behavioral conformance suite runs on native and in a real
browser, so that "works in the browser" is a measured claim rather than an assertion. The
suite runs in Chromium and Firefox, in both a Window and a Dedicated Worker context, and
the browser guide states which features are validated and which limits apply.

**Blocked by:** 02: Browser backend and WebAssembly surface

**Status:** ready-for-agent

- [ ] The conformance suite is written once against the public File System API and
      compiled for both native and the browser target; both builds run the same
      expectations.
- [ ] The suite covers File Entry create/overwrite/delete/rename, Content Revision
      identity and selection, retention of earlier revisions, Residency transitions,
      snapshot reconciliation with tombstones, admission and cancellation, and the File
      Session contract over a generic transport.
- [ ] The browser harness runs the suite in Chromium and Firefox, in both a Window and a
      Dedicated Worker context, following the sibling repository's harness structure and
      evidence capture.
- [ ] The browser-only contracts are asserted: exclusive ownership across two contexts of
      the same origin, retirement observed across contexts, and no observable partial
      revision after an interrupted write.
- [ ] The browser guide documents supported features, the storage lifecycle, quota and
      eviction limits, the validated engine baseline, and the explicitly unvalidated
      areas.
- [ ] The native and browser execution and metadata documentation describe the browser
      contracts alongside the native ones.
- [ ] The full native feature-powerset test command passes as the final validation.
