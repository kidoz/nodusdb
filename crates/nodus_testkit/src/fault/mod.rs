use thiserror::Error;

#[derive(Error, Debug)]
pub enum FaultInjectionError {
    #[error("Fault injected at {0:?}")]
    Injected(FaultPoint),
    #[error("Internal fault injector error: {0}")]
    Internal(String),
}

#[derive(Debug)]
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
    fn check_fault(&self, point: FaultPoint) -> Result<(), FaultInjectionError>;
}

// Scaffold dummy implementation
pub struct NoopFaultInjector;

impl FaultInjector for NoopFaultInjector {
    fn check_fault(&self, _point: FaultPoint) -> Result<(), FaultInjectionError> {
        Ok(())
    }
}
