# Fresh-disk recovery test contract

Fresh-disk recovery is not established by the existing storage tests.
`TestThreeProcessRhizaHA` restarts a stopped peer with the same data directory;
`TestOpenReloadsDynamicHandlerFromRhiza` also reopens its original directory.
Neither demonstrates recovery onto an empty disk.

A recovery integration test must first establish the recovery procedure from
the public API of the Rhiza version used by Conductor. It must use a disposable,
isolated object-store emulator and a new empty data directory, write and confirm
representative Conductor state before losing the original local disk, and then
recover through the supported API. After recovery, it must read back the
confirmed state through Conductor's storage API. It must fail if recovery or
verification fails; a successful open or readiness check alone is not proof that
the data was recovered. Cleanup must remove only resources created by the test.

The applicable S3, GCS, and Azure adapter contract tests must run against their
respective isolated emulators. Missing providers, unavailable emulator features,
or an undocumented recovery procedure are verification gaps, not passes. Do not
fall back to live cloud services. Emulator results do not establish production
cloud parity.

The recovery procedure and its durability boundary have not been verified here.
Until they are, fresh-disk recovery remains unverified, including which confirmed
writes are recoverable and whether peer assistance is required. Do not infer a
recovery guarantee from the existing same-directory restart tests.
