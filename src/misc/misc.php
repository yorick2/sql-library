<?php

namespace PaulMillband\SqlLibrary\misc;

class misc
{

    /**
     * @param string $table
     * @return string
     */
    public static function getTableColumnsSqlText(string $table) :string
    {
        return <<<SQL
    SELECT `COLUMN_NAME`
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = N'$table'
SQL;
    }

    /**
     * @param string $table
     * @param array $columnsToIgnore
     * @param string $separator
     * @return string
     */
    public static function getTableColumnNamesStringExcluding(string $table, array $columnsToIgnore, string $separator=',') :string
    {
        $columnsToIgnoreString = '`'.implode('`,`', $columnsToIgnore).'`';
        $query=<<<SQL
    SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR '$separator') as "columns excluding $columnsToIgnoreString"
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_NAME = N'$table'
SQL;
        for ($i = 0; $i < count($columnsToIgnore); $i++) {
            $query.= "\n    AND COLUMN_NAME != '$columnsToIgnore[$i]'";
        }
        return $query.';';
    }

    /**
     * @param string $table
     * @param array $tableRows
     * @param string $column
     * @param array $values
     * @return string
     */
     public static function getSelectAllWhereInArrayWithDuplicatesSqlText(string $table, array $tableRows, string $column, array $values):string
    {
        $columnList='`'.implode('`,`',$tableRows).'`';
        $query=<<<SQL
SELECT $columnList FROM `$table`
WHERE `$column`=$values[0]
SQL;
        for ($i = 0; $i < count($values); $i++) {
            $query.="\n".<<<SQL
            UNION ALL
            SELECT $columnList FROM `$table`
            WHERE `$column`=$values[$i]
            SQL;
        }
        return $query.';';
    }


    /**
     * @param string $destinationTable
     * @param array $destinationTableRows
     * @param string $sourceTable
     * @param array $sourceTableRows
     * @param string $column
     * @param array $values
     * @return string
     */
    public static function getInsertAllWhereInArrayWithDuplicatesSqlText(
        string $destinationTable,
        array  $destinationTableRows,
        string $sourceTable,
        array  $sourceTableRows,
        string $column,
        array $values
    ):string
    {
        $select=rtrim(
            self::getSelectAllWhereInArrayWithDuplicatesSqlText($sourceTable, $sourceTableRows, $column, $values),
            ';'
        );
        $columnList='`'.implode('`,`',$sourceTableRows).'`';
        $destinationTableList='`'.implode('`,`',$destinationTableRows).'`';
        return<<<SQL
INSERT INTO `$destinationTable`($destinationTableList) 
SELECT  $columnList FROM
        (
            $select
        ) AS t
SQL;
    }

    /**
     * @param string $table
     * @param array $columns
     * @return string
     */
    public static function getAllDuplicatesForColumnsSqlText(string $table, array $columns) :string
    {
        $columnsString = '`'.implode('`,`', $columns).'`';
        return<<<SQL
        select $columnsString, count(*) as NumDuplicates
        from $table
        group by $columnsString
        having NumDuplicates > 1
SQL;
    }
}
