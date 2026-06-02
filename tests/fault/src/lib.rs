pub use nodus_testkit::fault::{ConfigurableFaultInjector, FaultAction, FaultInjector, FaultPoint};

#[cfg(test)]
mod tests {
    use super::{ConfigurableFaultInjector, FaultAction, FaultInjector, FaultPoint};

    #[test]
    fn configurable_fault_injector_returns_error_at_configured_point() {
        let injector = ConfigurableFaultInjector::new();
        injector.inject(FaultPoint::BeforeWalSync, FaultAction::Error);

        assert!(injector.check_fault(FaultPoint::BeforeWalSync).is_err());
        assert!(injector.check_fault(FaultPoint::DuringCheckpoint).is_ok());
    }
}
