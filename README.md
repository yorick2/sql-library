# sql-library
## Definitions
### Simple Tables
import single table
e.g.

| x | y |
|---|---|
| 1 | 2 |
| 3 | 5 |
| 5 | 3 |

### One-many table
two tables linked table1:id = table2:table1_id. Overwriting table1 data for rows with the duplicate link column data

table1)

| id | text1 | text1b |
|----|-------|--------|
| 1  | test1 | foo3   |
| 4  | test2 | foo5   |
| 6  | test3 |        |

table2)

| id | text1_id | text2 |
|----|----------|-------|
| 1  | 1        | bar   |
| 4  | 1        | bar2  |
| 6  | 1        | bar3  |
| 6  | 4        | bar4  |
| 6  | 4        |       |
| 6  | 6        |       |

### Many-many table
two tables linked with a link/pivot table, importing every line of table data

link)

| table1_id | table2_id |
|-----------|-----------|
| 1         | 1         |
| 1         | 2         |
| 1         | 3         |
| 2         | 1         |
| 2         | 2         |
| 3         | 2         |
| 3         | 4         |

table1)

| id | text1 | text1b |
|----|-------|--------|
| 1  | test1 | bar    |
| 2  | test2 | barbar |
| 3  | test3 |        |

table2)

| id | text2 |
|----|-------|
| 1  | foo   |
| 2  | foo2  |
| 3  | foo3  |
| 4  | foo4  |

## Importers
src/Importer/SimpleImporter.php     import 1 table of data in a csv
- getSimpleManyManySqlText() : generate SQL to import a tsv or csv into a single table

src/Importer/OneManyImporter.php    import 2 tables of data in a csv, linked by column data provided
- getSimpleManyManySqlText() : generate SQL to import a tsv or csv into a one-many database
  
src/Importer/ManyManyImporter.php   import 2 tables of data in a csv, linked by a 3rd pivot table
- getSimpleManyManySqlText() : generate SQL to import a simple 1-1 
- getSplitRecordsWithCommasSqlText() : update an existing database by splitting rows where a column contains comma separated lists
- getPivotTableImportSqlText() : generate SQL to import pivot table


## Reducers
Delete rows from 2 tables linked together, leaving the defined qty of rows. Leaving valid linked data.
src/reducer/OneManyImporter.php 
src/reducer/ManyManyImporter.php

## Testing
### native
add test files to a native setup for sql to import during tests
cp "<<path to repo>>/tests/data" /tmp

### Docker
add test files to docker sql instance to import during tests
docker cp "<<path to repo>>/tests/data" /tmp
e.g.
docker cp "<<path to repo>>/tests/data" mysql_1:/tmp