use declarative_dataflow::server::scheduler::{Event, Scheduler};
use std::time::Duration;

#[test]
fn test_schedule_now() {
    let mut scheduler = Scheduler::new();

    assert!(!scheduler.has_pending());
    assert!(scheduler.until_next().is_none());

    scheduler.event_after(Duration::from_secs(0), Event::Tick);

    assert!(scheduler.has_pending());
    assert_eq!(scheduler.next().unwrap().schedule(), Some(Event::Tick));
}

#[test]
fn test_schedule_after() {
    let mut scheduler = Scheduler::new();

    scheduler.event_after(Duration::from_secs(2), Event::Tick);

    assert!(!scheduler.has_pending());
    assert!(scheduler.next().is_none());
    assert!(scheduler.until_next().is_some());

    std::thread::sleep(scheduler.until_next().unwrap());

    assert!(scheduler.has_pending());
    assert_eq!(scheduler.next().unwrap().schedule(), Some(Event::Tick));
    assert!(scheduler.until_next().is_none());
}
