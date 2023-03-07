set dialect='kusto';

print '-- #1 --';
print 1, 2, 3;
print 1, 2, 3 | getschema;

print '-- #2 --';
print 1, y = 2, 3;
print 1, y = 2, 3 | getschema;
