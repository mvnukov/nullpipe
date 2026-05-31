// REFACTORING PLAN FOR room.rs
// ============================
//
// GOAL: Transform room.rs from a monolithic "god module" into a thin orchestrator
// that delegates low-level networking to hub.rs and joiner.rs.
//
// CURRENT STATE:
// - Contains duplicate implementations of Host and Joiner logic.
// - Bypasses Hub::run() and Joiner::run() with its own buggy loops.
// - Uses tokio::io::split directly, causing nullpipe-2vp (Tor write failure).
//
// TARGET STATE:
// 1. host() -> Bootstrap Tor -> Create Hub -> Hub::run()
// 2. join() -> Bootstrap Tor -> Create Joiner -> Joiner::connect() -> Joiner::run()
//
// STEPS:
// 1. [DONE] Fix tokio::io::split in joiner.rs (handshake).
// 2. [TODO] Fix tokio::io::split in joiner.rs (run_connected).
// 3. [TODO] Fix tokio::io::split in hub.rs (peer handling).
// 4. [TODO] Delete run_joiner_loop() from this file.
// 5. [TODO] Delete host peer-handling split logic from this file.
// 6. [TODO] Update host_task() to use Hub::new() and Hub::run().
// 7. [TODO] Update joiner_task() to use Joiner::connect() and Joiner::run().
//
// BUG TRACKING:
// - nullpipe-2vp: Joiner can't write back to Host across independent bootstraps.
//   Root cause: tokio::io::split on arti_client::DataStream.
//   Fix: Replace split with single-owner sequential I/O or Arc<Mutex> with short locks.
