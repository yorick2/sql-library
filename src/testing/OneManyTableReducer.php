<?php

namespace Yorick2\SqlLibrary\testing;

class OneManyTableReducer
{
    /**
     * delete table rows in given tables to leave the defined number of remaining rows from table1,
     * with all linked rows in table2
     *
     * @param int $qtyRows number of rows to keep in table1
     * @param string $table1 e.g. 'table1'
     * @param string $linkColumn1 column in this table that links to table 2 e.g. 'id'
     * @param string $table2 e.g. 'table2'
     * @param string $linkColumn2 column in this table that links to table 1 e.g. 'table1_id'
     * @return string
     */
    public function getSqlText(
        int $qtyRows,
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
                   LIMIT 1 OFFSET $qtyRows
               ) AS lim
            ON  p.`id` > lim.`id`;
        DELETE FROM `$table2`
            WHERE `$linkColumn2` NOT IN
                (SELECT `$linkColumn1` FROM `$table1`);
EOF;
    }
}
