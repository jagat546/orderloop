# Licence options

Not decided. The owner chooses before the repository goes public, after the employment-contract and IP check, and records the choice with `/record-decision`. Until then the repository has no `LICENSE` file, which means all rights are reserved. This is not legal advice.

Both options are OSI-approved, and both can include the project's dependencies, which are mostly Apache-2.0 (Spring, Kafka clients) or BSD (PostgreSQL JDBC). Confirm each dependency's licence before publishing.

## Apache-2.0

Permissive: anyone may use, modify, redistribute and sell it, including in closed-source products, if they keep the licence and notices and mark changed files.

| Gains | Costs |
|---|---|
| Widest adoption. Most companies allow Apache-2.0 dependencies without legal review | Anyone can run a closed, hosted fork and compete with your own hosted offering, giving nothing back |
| Explicit patent grant from contributors, ending if the licensee sues over patents | A later move to a stricter licence covers only new versions; released versions stay Apache-2.0 |
| Simple for contributors; the inbound licence can be the same as the outbound one | Little leverage for dual licensing, since anyone may already use it commercially |
| Code can move into GPLv3 or AGPLv3 projects (not the reverse) | |

## AGPL-3.0

Strong copyleft with a network clause: anyone who modifies it and lets users interact with it over a network must offer those users the modified source under AGPL-3.0.

| Gains | Costs |
|---|---|
| A hosted competitor must publish its changes, which protects a future hosted offering | Many companies ban AGPL software by policy, which reduces adoption and outside contributions |
| Allows dual licensing (AGPL for everyone, a commercial licence for those who want to avoid copyleft), if you hold or are licensed all copyright | Dual licensing needs a contributor licence agreement or copyright assignment, which deters some contributors |
| Includes a patent licence from contributors (section 11) | Operators must provide a way to get the source of any modified version they host, which is extra work for self-hosters who customise it |
| | Its effect on code that only talks to it over HTTP or Kafka is debated, so integrators may ask for legal review |

## Questions that decide it

1. Do you intend to sell a hosted version? If yes, AGPL-3.0 protects that; if no, Apache-2.0 maximises reach.
2. Do you want company contributors? Apache-2.0 makes that far easier.
3. Does your employment contract affect who owns this code? Answer that first.
