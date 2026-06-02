use thiserror::Error;

#[derive(Error, Debug)]
pub enum FaultInjectionError {
    #[error("Fault injected at {0:?}")]
    Injected(FaultPoint),
    #[error("Internal fault injector error: {0}")]
    Internal(String),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
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

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

pub trait FaultInjector: Send + Sync {
    fn check_fault(&self, point: FaultPoint) -> Result<(), FaultInjectionError>;
}

#[derive(Debug, Clone)]
pub enum FaultAction {
    Error,
    Panic(String),
    Delay(Duration),
}

pub struct ConfigurableFaultInjector {
    faults: RwLock<HashMap<FaultPoint, FaultAction>>,
}

impl ConfigurableFaultInjector {
    pub fn new() -> Self {
        Self {
            faults: RwLock::new(HashMap::new()),
        }
    }

    pub fn inject(&self, point: FaultPoint, action: FaultAction) {
        let mut guard = self.faults.write().unwrap();
        guard.insert(point, action);
    }

    pub fn clear(&self, point: &FaultPoint) {
        let mut guard = self.faults.write().unwrap();
        guard.remove(point);
    }
}

impl Default for ConfigurableFaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl FaultInjector for ConfigurableFaultInjector {
    fn check_fault(&self, point: FaultPoint) -> Result<(), FaultInjectionError> {
        let action = {
            let guard = self.faults.read().unwrap();
            guard.get(&point).cloned()
        };

        match action {
            Some(FaultAction::Error) => Err(FaultInjectionError::Injected(point)),
            Some(FaultAction::Panic(msg)) => panic!("Injected panic at {:?}: {}", point, msg),
            Some(FaultAction::Delay(duration)) => {
                std::thread::sleep(duration);
                Ok(())
            }
            None => Ok(()),
        }
    }
}

// Scaffold dummy implementation
pub struct NoopFaultInjector;

impl FaultInjector for NoopFaultInjector {
    fn check_fault(&self, _point: FaultPoint) -> Result<(), FaultInjectionError> {
        Ok(())
    }
}
