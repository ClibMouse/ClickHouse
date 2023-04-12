DROP TABLE IF EXISTS TableWithVariousDataTypes;
CREATE TABLE TableWithVariousDataTypes
(
    Name String,
    Age Nullable(UInt8),
    Height Float64,
    JoinDate DateTime64(9, 'UTC')
) engine = Memory;

INSERT INTO TableWithVariousDataTypes VALUES ('A', 12, 5.2, '2020-01-01'), ('B', 22, 7.2, '2020-01-02'), ('C', 32, 9.3, '2021-12-31');

set dialect='kusto';

print '-- operator between, !between --';
TableWithVariousDataTypes | project Age | where Age between (10 .. 12);
TableWithVariousDataTypes | project Age | where Age !between (10 .. 30);
TableWithVariousDataTypes | project Height | where Height between (5.2 .. 6.6);
TableWithVariousDataTypes | project Height | where Height !between (5.3 .. 7.6);
TableWithVariousDataTypes | project JoinDate | where todatetime(JoinDate) between (datetime('2020-01-01') .. 2d);
TableWithVariousDataTypes | project JoinDate | where JoinDate !between (datetime('2020-01-01') .. 2d);
TableWithVariousDataTypes | project JoinDate | where JoinDate between (datetime('2020-06-30') .. datetime('2025-06-30'));
TableWithVariousDataTypes | project JoinDate | where JoinDate !between (datetime('2020-06-30') .. datetime('2025-06-30'));
TableWithVariousDataTypes | project Age | where Age between (10 .. 12) or Age > 30;
TableWithVariousDataTypes | project Age | where Age between (10 .. 12) or Age between (30 .. 50);
range x from 1 to 100 step 1 | where x between ( 8 + (4 * 2) / 8 + ((5 * 5 - 20) * 2) .. 20);
range x from 1 to 100 step 1 | where x between ( 8 + (4 * 2) / 8 + ((5 * 5 - 20) * 2) ... 20); -- { clientError SYNTAX_ERROR }
range x from 1 to 100 step 1 | where x between ( 8 + (4 * 2) / 8 + ((5 * 5 - 20) * 2) . 20); -- { clientError SYNTAX_ERROR }
range x from 1 to 100 step 1 | where x between ( 8 + (4 * 2) / 8 + ((5 * 5 - 20) * 2)  20); -- { clientError SYNTAX_ERROR }
range x from 1 to 100 step 1 | where x between (50 .. '58'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range x from 1 to 100 step 1 | where x between ('50' .. 58); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range x from 1 to 100 step 1 | where x between ('50' .. '58'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range x from 0.1 to 1 step 0.1 | where x between (0.4 .. .7);
range x from 1 to 100 step 1 | where x between (50 .. datetime(2007-07-27)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
range x from 1 to 100 step 1 | where x between (datetime(2007-07-27) .. datetime(2007-07-30)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
