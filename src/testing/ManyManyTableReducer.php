<?php

namespace PaulMillband\SqlLibrary\testing;

class ManyManyTableReducer
{
    /**
     * delete table rows in given tables to leave the defined number of remaining rows from table1,
     * with all linked rows in table2 and pivotTable
     *
     * @param string $pivotTable string e.g. 'table1_table2'
     * @param string $table1 e.g. 'table1'
     * @param string $linkColumn1 column in this table that links to $pivotTable e.g. 'table1_id'
     * @param string $table2 e.g. 'table2'
     * @param string $linkColumn2 column in this table that links to $pivotTable e.g. 'table2_id'
     * @return string
     */
    static function getSqlText(
        string $pivotTable,
        string $table1,
        string $linkColumn1,
        string $table2,
        string $linkColumn2
    )
    {
        return <<<EOF
        DELETE FROM
               `$table1` AS p
            JOIN
               ( SELECT `id`
                 FROM `$table1`
                 ORDER BY `id` ASC
                   LIMIT 1 OFFSET 4
               ) AS lim
            ON  p.`id` > lim.`id` ;
        DELETE FROM `$pivotTable`
            WHERE `$linkColumn1` NOT IN
                (SELECT `id` FROM `$table1`);
        DELETE FROM `$table2`
            WHERE `id` NOT IN
                (SELECT `$linkColumn2` FROM `$pivotTable`);
EOF;
    }
}
