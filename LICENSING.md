# Licensing and third-party notices

## Ibex

Unless a file states otherwise, Ibex is Copyright (C) 2026 Bob Jansen and is
licensed under the GNU Affero General Public License, version 3 only
(`AGPL-3.0-only`).  The complete license text is in [`LICENSE`](LICENSE).

AGPLv3 is intended to keep externally provided, modified Ibex services open:
when a modified version supports remote network interaction, its operator must
offer the Corresponding Source of that version to the users who interact with
it (AGPLv3 section 13).  This repository does not add a separate SaaS-use
restriction or a custom "commercial use" condition; doing so would be
incompatible with the standard AGPL grant.

Bob Jansen owns the copyright in Ibex-authored material.  Contributor
material remains owned by its respective authors, but accepted contributors
grant Bob Jansen the rights described in the Contributor License Agreement
([`.github/CLA.md`](.github/CLA.md)), including the right to include their
contributions in separately licensed commercial versions of Ibex.

Commercial licenses may be offered by Bob Jansen under his own copyright and
the relicensing rights granted by contributors.  A commercial license is only
available by a separate written agreement; it is not granted by this
repository or by the AGPL.  Do not submit code that is subject to terms that
prevent the CLA grant; see [`CONTRIBUTING.md`](CONTRIBUTING.md) for the
contribution process.

This is a project licensing notice, not legal advice.  Obtain counsel before
selling commercial licenses or enforcing rights in a particular jurisdiction.

## ribex and Poorman-derived material

The `r/ribex` package is part of Ibex and is AGPL-3.0-only except for the
Poorman-derived test material described below.  Its package metadata points to
the repository-level [`LICENSE`](LICENSE).

The Poorman-derived conformance-test material under `r/ribex/tests/` is
separately attributed in `r/ribex/tests/poorman-c9eb1f1.md`.  The original
Poorman license is MIT and is preserved at
`r/ribex/inst/third-party/poorman/LICENSE`:

- Copyright (c) 2020 Nathan Eastwood.
- The MIT notice remains applicable to the Poorman-derived material.
- That material is not asserted to be Copyright (C) 2026 Bob Jansen and must
  not be included in a commercial Ibex license unless the required rights and
  notices have been separately confirmed.

The full tracked Poorman snapshot is not vendored; only adapted test cases and
the required MIT attribution are present.
