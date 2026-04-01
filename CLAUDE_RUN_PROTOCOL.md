\# CLAUDE RUN PROTOCOL — BID GUARDRAIL



\## PURPOSE

Prevent destructive or uncontrolled changes when using Claude with dangerously-skip-permissions.



\---



\## 🔒 RULE 1 — ALWAYS CREATE CHECKPOINT BEFORE WORK



Before ANY meaningful change:



git add .

git commit -m "checkpoint: pre-<mission-name>"

git push



If you skip this, you accept risk of losing working state.



\---



\## 🔒 RULE 2 — NEVER RUN UNBOUNDED



Claude must ALWAYS be given:

\- a mission file

\- clear scope

\- stop conditions



No open-ended instructions like:

"fix matching" or "improve system"



\---



\## 🔒 RULE 3 — DO NOT REWRITE WHOLE MODULES



Allowed:

\- targeted fixes

\- small edits

\- alias additions

\- mapping additions



Not allowed:

\- rewriting ingest pipeline

\- rewriting reconciliation logic wholesale

\- restructuring folders



\---



\## 🔒 RULE 4 — PROTECT CORE FILES



These files are HIGH RISK:



backend/app/quote\_reconciliation/rules.py

backend/app/quote\_reconciliation/ingest.py

backend/app/bid\_validation/normalize.py



Claude must:

\- explain BEFORE changing

\- keep changes minimal



\---



\## 🔒 RULE 5 — TESTS MUST PASS AFTER CHANGE



After ANY change:



python -m pytest tests/ -v



If tests fail:

\- STOP

\- do not continue stacking changes



\---



\## 🔒 RULE 6 — NO SILENT BEHAVIOR CHANGES



Claude must:

\- explain what changed

\- explain why

\- link change to a failing test or known issue



\---



\## 🔒 RULE 7 — SMALL STEPS ONLY



Each mission should:

\- fix ONE class of problem

\- not multiple layers at once



\---



\## 🔒 RULE 8 — DO NOT TOUCH FIXTURES/TRUTH



Files under:



tests/fixtures/adel\_ipsi/



are \*\*canonical truth\*\*



Do NOT modify them to make tests pass.



\---



\## 🔒 RULE 9 — IF UNSURE, STOP



Claude must stop and report if:

\- change is larger than expected

\- behavior unclear

\- multiple fixes required



\---



\## 🔒 RULE 10 — NO “HELPFUL IMPROVEMENTS”



Claude must NOT:

\- optimize code

\- refactor structure

\- clean unrelated logic



Only do what mission specifies.



\---



\## EXECUTION MODE



If using:



claude --dangerously-skip-permissions



You MUST follow this protocol.

