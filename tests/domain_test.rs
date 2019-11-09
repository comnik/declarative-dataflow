use timely::dataflow::operators::UnorderedInput;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::trace::TraceReader;

use declarative_dataflow::domain::{AsSingletonDomain, Domain};
use declarative_dataflow::Value;

type Aid = &'static str;

#[test]
fn test_advance_epoch() {
    let mut domain = Domain::<Aid, u64>::new(0);
    assert_eq!(domain.epoch(), &0);

    assert!(domain.advance_epoch(1).is_ok());
    assert_eq!(domain.epoch(), &1);

    assert!(domain.advance_epoch(1).is_ok());
    assert_eq!(domain.epoch(), &1);

    assert!(domain.advance_epoch(0).is_err());
    assert_eq!(domain.epoch(), &1);
}

#[test]
fn test_advance_only_epoch() {
    timely::execute_directly(move |worker| {
        let (domain, _handle, _cap) = worker.dataflow::<u64, _, _>(|scope| {
            let tx_test: Domain<Aid, u64> = scope
                .new_unordered_input::<((Value, Value), u64, isize)>()
                .as_singleton_domain("tx_test")
                .into();

            let ((handle, cap), source) =
                scope.new_unordered_input::<((Value, Value), u64, isize)>();

            let source_test: Domain<Aid, u64> = source.as_singleton_domain("source_test").into();

            (tx_test + source_test, handle, cap)
        });

        assert_eq!(domain.probed_source_count(), 1);
        assert_eq!(domain.epoch(), &0);
        assert!(!domain.dominates(AntichainRef::new(&[0])));

        // tick tx => stalls!
        // domain.advance_epoch(1).unwrap();
        // worker.step_while(|| !domain.dominates(AntichainRef::new(&[0])));
        // domain.advance().unwrap();
    });
}

#[test]
fn test_advance_only_source() {
    timely::execute_directly(move |worker| {
        let (mut domain, _handle, mut cap): (Domain<Aid, u64>, _, _) = worker
            .dataflow::<u64, _, _>(|scope| {
                let ((handle, cap), source) =
                    scope.new_unordered_input::<((Value, Value), u64, isize)>();

                let source_test: Domain<Aid, u64> = source
                    .as_singleton_domain("source_test")
                    .with_slack(1)
                    .into();

                let tx_test: Domain<Aid, u64> = scope
                    .new_unordered_input::<((Value, Value), u64, isize)>()
                    .as_singleton_domain("tx_test")
                    .with_slack(1)
                    .into();

                (source_test + tx_test, handle, cap)
            });

        assert_eq!(domain.attributes.len(), 2);
        assert_eq!(domain.probed_source_count(), 1);
        assert_eq!(domain.epoch(), &0);
        assert!(!domain.dominates(AntichainRef::new(&[])));
        assert!(!domain.dominates(AntichainRef::new(&[0])));
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .distinguish_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .distinguish_frontier(),
            &[0]
        );

        // tick
        cap.downgrade(&1);
        worker.step_while(|| !domain.dominates(AntichainRef::new(&[0])));
        domain.advance().unwrap();

        assert_eq!(domain.epoch(), &1);
        assert!(domain.dominates(AntichainRef::new(&[0])));
        assert!(!domain.dominates(AntichainRef::new(&[1])));
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .distinguish_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .distinguish_frontier(),
            &[0]
        );

        // tick
        cap.downgrade(&2);
        worker.step_while(|| !domain.dominates(AntichainRef::new(&[1])));
        domain.advance().unwrap();

        assert_eq!(domain.epoch(), &2);
        assert!(domain.dominates(AntichainRef::new(&[1])));
        assert!(!domain.dominates(AntichainRef::new(&[2])));
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .advance_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("tx_test")
                .unwrap()
                .distinguish_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .advance_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward_propose
                .get_mut("source_test")
                .unwrap()
                .distinguish_frontier(),
            &[1]
        );
    });
}
