[![License][ico-license]](LICENSE.md)
![Release][ico-in-development]
![Release][ico-release]
![Release][ico-tag]
![Download Size][ico-download-size]
![Last Commit][ico-last-commit]

[![PHPUnit](https://github.com/yorick2/sql-library/actions/workflows/phpunit.yml/badge.svg?branch=master)](https://github.com/yorick2/sql-library/actions/workflows/phpunit.yml)

![Github Top Language][ico-top-language]

![PHP version][ico-php-version]
![PHPUNIT version][ico-phpunit-version]
![COMPOSER version][ico-composer-version]

[ico-license]: https://img.shields.io/badge/license-MIT-brightgreen.svg?style=for-the-badge
[ico-in-development]: https://img.shields.io/badge/Release-Development-yellow?style=for-the-badge
[ico-release]: https://img.shields.io/github/v/release/yorick2/sql-library?style=for-the-badge
[ico-tag]: https://img.shields.io/github/v/tag/yorick2/sql-library?style=for-the-badge
[ico-download-size]: https://img.shields.io/github/languages/code-size/yorick2/sql-library?style=for-the-badge
[ico-last-commit]: https://img.shields.io/github/last-commit/yorick2/sql-library?style=for-the-badge

[ico-top-language]: https://img.shields.io/github/languages/top/yorick2/sql-library?style=for-the-badge&logoColor=white
[ico-php-version]: https://img.shields.io/badge/PHP%208.1-777BB4?style=for-the-badge&logo=php&logoColor=white
[ico-phpunit-version]: https://img.shields.io/badge/PHPUnit-777BB4?style=for-the-badge&logoColor=white
[ico-composer-version]: https://img.shields.io/badge/composer-885630?style=for-the-badge&logo=composer&logoColor=white

[<img src="https://www.paypalobjects.com/en_GB/i/btn/btn_donate_LG.gif">](https://www.paypal.com/donate/?hosted_button_id=95TTM4Z9Q7MNG)

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
- getSqlText() : generate SQL to import a tsv or csv into a single table
- getSplitRecordsWithCommasSqlText() : update an existing database by splitting rows where a column contains comma separated lists
- getSplitRecordsWithMultipleCommaColumnsSqlText() : update an existing database by splitting rows where multiple columns contain comma separated lists. Where the first item has data from before the first comma in all split columns, the second item has data after the first comma in both columns, ...

src/Importer/OneManyImporter.php    import 2 tables of data in a csv, linked by column data provided
- getSqlText() : generate SQL to import a tsv or csv into a one-many database
- getAddColumnSqlText : generate SQL to import a new table in a one many connection
  
src/Importer/ManyManyImporter.php   import 2 tables of data in a csv, linked by a 3rd pivot table
- getSqlText() : generate SQL to import a simple 1-1 
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
