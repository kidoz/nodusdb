pub enum FaultPoint {
    BeforeWalSync,
    AfterWalSyncBeforeAck,
    DuringCheckpoint,
    BeforeRaftAppend,
    AfterRaftCommitBeforeApply,
    DuringIndexBackfill,
    DuringIndexValidation,
    DuringBackupManifestWrite,
    DuringRestoreChunkApply,
    DuringUpgradeFinalization,
    AfterGrantBeforeCatalogPublish,
    AfterRevokeBeforeCacheInvalidation,
}

pub trait FaultInjector: Send + Sync {
    fn check_fault(&self, point: FaultPoint) -> anyhow::Result<()>;
}

// Scaffold dummy implementation
pub struct NoopFaultInjector;

impl FaultInjector for NoopFaultInjector {
    fn check_fault(&self, _point: FaultPoint) -> anyhow::Result<()> {
        Ok(())
    }
}
