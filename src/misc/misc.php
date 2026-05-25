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
            $query.= "\n    AND COLUMN_NAME != '$columnsToIgnore[0]'";
        }
        return $query.';';
    }
}
