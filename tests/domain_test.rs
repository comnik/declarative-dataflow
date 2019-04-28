use timely::dataflow::operators::UnorderedInput;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::trace::TraceReader;

use declarative_dataflow::domain::Domain;
use declarative_dataflow::Value;
use declarative_dataflow::{AttributeConfig, InputSemantics};

#[test]
fn test_advance_epoch() {
    let mut domain = Domain::<u64>::new(0);
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
        let mut domain = Domain::<u64>::new(0);

        let (_handle, mut cap) = worker.dataflow::<u64, _, _>(|scope| {
            let ((handle, cap), pairs) =
                scope.new_unordered_input::<((Value, Value), u64, isize)>();

            domain
                .create_sourced_attribute(
                    "source_test",
                    AttributeConfig::tx_time(InputSemantics::Raw),
                    &pairs,
                )
                .unwrap();

            domain
                .create_transactable_attribute(
                    "tx_test",
                    AttributeConfig::tx_time(InputSemantics::Raw),
                    scope,
                )
                .unwrap();

            (handle, cap)
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
        let mut domain = Domain::<u64>::new(0);

        let (_handle, mut cap) = worker.dataflow::<u64, _, _>(|scope| {
            let ((handle, cap), pairs) =
                scope.new_unordered_input::<((Value, Value), u64, isize)>();

            domain
                .create_sourced_attribute(
                    "source_test",
                    AttributeConfig::tx_time(InputSemantics::Raw),
                    &pairs,
                )
                .unwrap();

            domain
                .create_transactable_attribute(
                    "tx_test",
                    AttributeConfig::tx_time(InputSemantics::Raw),
                    scope,
                )
                .unwrap();

            (handle, cap)
        });

        assert_eq!(domain.probed_source_count(), 1);
        assert_eq!(domain.epoch(), &0);
        assert!(!domain.dominates(AntichainRef::new(&[])));
        assert!(!domain.dominates(AntichainRef::new(&[0])));
        assert_eq!(
            domain
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .distinguish_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
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
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .distinguish_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[0]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
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
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("tx_test")
                .unwrap()
                .propose_trace
                .distinguish_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
                .advance_frontier(),
            &[1]
        );
        assert_eq!(
            domain
                .forward
                .get_mut("source_test")
                .unwrap()
                .propose_trace
                .distinguish_frontier(),
            &[1]
        );
    });
}
