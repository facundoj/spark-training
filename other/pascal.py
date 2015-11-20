#!/usr/bin/python
# -*- coding: utf-8 -*-


# Returns first n lines of the pascal triangle: O((n²+n) / 2) --> O(n²)
def pascal_triangle(n):
    lines = []
    prev_line = None
    for i in range(1, n + 1):
        line = []
        for j in range(0, i):
            if j == 0 or j == (i - 1):
                line.append(1)
            else:
                line.append(prev_line[j - 1] + prev_line[j])
        lines.append(line)
        prev_line = line

    return lines

# Tests

print '\n'.join(
    map(lambda line: ' '.join(
        map(lambda n: str(n), line)),
        pascal_triangle(-3))
)

print '\n'.join(
    map(lambda line: ' '.join(
        map(lambda n: str(n), line)),
        pascal_triangle(0))
)

print '\n'.join(
    map(lambda line: ' '.join(
        map(lambda n: str(n), line)),
        pascal_triangle(8))
)