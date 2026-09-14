-- SPDX-License-Identifier: AGPL-3.0-only
-- Copyright (C) 2026 Bob Jansen
--
-- One table with a column per PostgreSQL type the walkthrough checks. Row 3 is
-- NULL everywhere except id and label. Loaded with psql inside the container,
-- so seeding does not depend on the ADBC driver being tested.

drop table if exists ibex_types;

create table ibex_types (
    id      integer primary key,
    small_n smallint,
    int_n   integer,
    big_n   bigint,
    dbl     double precision,
    real_n  real,
    price   numeric(12, 2),
    flag    boolean,
    label   text,
    code    varchar(8),
    day     date,
    ts      timestamp,
    tstz    timestamptz,
    doc     jsonb,
    bytes   bytea,
    uid     uuid,
    t       time,
    iv      interval,
    tags    text[]
);

insert into ibex_types values
    (1, 1, 100, 10000000000, 1.5, 1.5, 12.34, true, 'alpha', 'A1',
     '2026-01-02', '2026-01-02 03:04:05.123456', '2026-01-02 03:04:05.123456+00',
     '{"k": 1}', '\x0102', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
     '12:30:00', '1 day 2 hours', '{x,y}'),
    (2, -2, -200, -20000000000, -2.25, -2.25, -0.50, false, 'beta', 'B2',
     '1999-12-31', '1999-12-31 23:59:59', '1999-12-31 23:59:59+01',
     '[1, 2]', '\x', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',
     '00:00:01', '3 months', '{}'),
    (3, null, null, null, null, null, null, null, 'alpha', null,
     null, null, null,
     null, null, null,
     null, null, null);
