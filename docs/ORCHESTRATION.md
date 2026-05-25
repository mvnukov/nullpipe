# Orchestration Checklist — Ephemeral Chat Build Through

## Overview
Work through remaining phases sequentially. Each phase gets two Pi instances: implement → verify/fix → commit → mark done → next phase.

---

## Rules
- **Kill previous instance BEFORE spawning a new one** — avoids duplicate session names and confusion
- **Target sessions by PID** — Pi sessions reuse names (e.g. "nullpipe"), PID is unique
- **Keep prompts minimal** — task files have all details. Just say `Implement docs/tasks/phaseX.md. Reply when done.` or `Verify docs/tasks/phaseX.md. Reply when done.`
- **Check every 5 minutes** — if no reply within 5 min, send a status check. If still no reply, check tmux pane output.
- **Fallback if agent forgets to reply:**
  1. `send_to_session` — "done yet?"
  2. `tmux capture-pane` — check if sitting at shell prompt
  3. If stuck — ask user to inspect the pane
- **No code review** — orchestrator doesn't review code. Spawn, wait, mark, repeat.

---

## Loop: Per-Phase Workflow

### Step 1: KILL previous instance
- Find old Pi session targeting `/Users/mikhail/Workspace/nullpipe` via `list_sessions`
- Kill its tmux pane: `tmux kill-pane -t <pane-id>` or kill the PID

### Step 2: SPAWN Implementer
- `tmux split-window -h 'cd /Users/mikhail/Workspace/nullpipe && pi'`
- Wait for it to appear in `list_sessions`, note its PID
- Send task: `Implement docs/tasks/phaseX.md. Reply when done.`
- Wait for reply

### Step 3: KILL Implementer
- Kill its tmux pane

### Step 4: SPAWN Verifier
- `tmux split-window -h 'cd /Users/mikhail/Workspace/nullpipe && pi'`
- Wait for it to appear in `list_sessions`, note its PID
- Send task: `Verify docs/tasks/phaseX.md. Reply when done.`
- Wait for reply (it commits and pushes on success)

### Step 5: KILL Verifier
- Kill its tmux pane

### Step 6: MARK DONE
- Update `docs/PLAN.md` — check off all completed tasks for this phase
- Update the corresponding `docs/tasks/phaseX.md` — mark all items and verification checks as `[x]`

### Step 7: NEXT PHASE
- Repeat from Step 1 with the next pending phase

---

## Stopping Conditions
- All phases 4-8 complete
- Any phase that cannot be completed after 2 verify/fix cycles → note blocker and continue
- Build breaks that can't be fixed → stop and report

## Tmux Pane Naming Convention
- `pi-implement-4`, `pi-verify-4`, `pi-implement-5`, `pi-verify-5`, etc.