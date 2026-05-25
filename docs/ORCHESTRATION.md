# Orchestration Checklist — Ephemeral Chat Build Through

## Overview
Work through remaining phases sequentially. Each phase gets two Pi instances: implement → verify/fix → commit → mark done → next phase.

## Current Status
- Phase 0-3: ✅ Done
- Phase 4: Partially done (wire.rs + joiner.rs exist; hub broadcast, robustness, verification pending)
- Phase 5: Not started
- Phase 6 (CLI): Not started (in PLAN.md, no task file)
- Phase 7 (TUI): Not started (in PLAN.md, no task file)
- Phase 8 (Integration): Not started (in PLAN.md, no task file)

---

## Loop: Per-Phase Workflow

### Step 1: SPAWN Implementer
- Open new tmux pane
- Start Pi in `/Users/mikhail/Workspace/nullpipe`
- Task prompt:
  ```
  Implement the next pending phase from docs/tasks/.
  Read the phase task file (e.g. docs/tasks/phase4.md).
  Check existing code first — some parts may already be implemented.
  Implement all unchecked items in the task file.
  Run: cargo check, cargo clippy -D warnings, cargo fmt --check, cargo test
  Fix any failures. Do NOT mark tasks done or commit. Just implement and verify locally.
  When done, exit cleanly.
  ```
- Wait for completion (watch tmux pane or poll)

### Step 2: CLOSE Implementer
- Kill the tmux pane/Pi instance

### Step 3: SPAWN Verifier
- Open new tmux pane
- Start Pi in `/Users/mikhail/Workspace/nullpipe`
- Task prompt:
  ```
  Verify the Phase X implementation.
  Read docs/tasks/phaseX.md and check every verification item.
  Run all checks: cargo check, cargo clippy -D warnings, cargo fmt --check, cargo test
  If any check fails, fix the code.
  If verification items are incomplete, implement them.
  When everything passes, commit with message "feat: phase X - <summary>"
  Then push to remote.
  Do NOT mark tasks done in PLAN.md. Just commit and push.
  When done, exit cleanly.
  ```
- Wait for completion

### Step 4: CLOSE Verifier
- Kill the tmux pane/Pi instance

### Step 5: MARK DONE
- Update `docs/PLAN.md` — check off all completed tasks for this phase
- Update the corresponding `docs/tasks/phaseX.md` — mark all items and verification checks as `[x]`

### Step 6: NEXT PHASE
- Repeat from Step 1 with the next pending phase

---

## Phase Order
1. **Phase 4** — Core: message broadcast (partially implemented)
   - Already exists: `wire.rs` (wire protocol), `joiner.rs` (send/recv)
   - Needs: hub broadcast channel, integration, stream robustness, verification
2. **Phase 5** — Core: RoomHandle and EventStream (not started)
3. **Phase 6** — Binary: CLI (not started, in PLAN.md only)
4. **Phase 7** — Binary: TUI (not started, in PLAN.md only)
5. **Phase 8** — Integration and polish (not started, in PLAN.md only)

## Stopping Conditions
- All phases 4-8 complete
- Any phase that cannot be completed after 2 verify/fix cycles → note blocker and continue
- Build breaks that can't be fixed → stop and report

## Tmux Pane Naming Convention
- `pi-implement-4`, `pi-verify-4`, `pi-implement-5`, `pi-verify-5`, etc.