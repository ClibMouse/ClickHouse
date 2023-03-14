set dialect='kusto';
print ' -- hash --';
print hash('World');
print hash('World', 100);
print hash(datetime("2015-01-01"));
print hash(-1);
print hash(int(-1));
print hash(long(-1));
print hash(real(-1));
print hash(-1, 100);
print hash(-1, -1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
print ' -- hash_sha256 --';
print hash_sha256('World');
print hash_sha256(datetime(2020-01-01));
print hash_sha256(1);
print hash_sha256(-1);
