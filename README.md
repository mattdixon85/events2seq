# events2seq

A repo with SQL-solutions for turning event streams into sequences.

So given an event stream like so (possibly not stored in time order):

| id | ts | event |
-------------------
| 1  | 1  | foo   |
| 2  | 10 | bar   |
| 1  | 2  | bas   |
| 1  | 0  | qux   |
| 2  | 3  | foo   |

the query should return a row for each unique id in the 'id' field with the values of the 'event' field in a new column 'seq', where the events are presented in a space-separated string, in order relative to the column 'ts'):

| id | seq             |
------------------------
| 1  | qux foo bar bas |
| 2  | foo bar         |
