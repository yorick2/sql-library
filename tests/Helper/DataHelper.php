<?php

namespace PaulMillband\SqlLibrary\tests\Helper;

use function PHPUnit\Framework\throwException;

class DataHelper
{
    /**
     * @param array $data1 an associated array in format [ ["col1"=>"1","col2"=>"9"], ["col1"=>"4","col2"=>"6"], ... ]
     * @param array $data2 an associated array in format [ ["col1"=>"1","col2"=>"9"], ["col1"=>"4","col2"=>"6"], ... ]
     * @param bool $findAll find all missing items. Default is to stop when first missing row found
     * @return array returns first data row that cant be found in the csv
     */
    static function getDataMissing(array $data1, array $data2, bool $findAll=false): array
    {
        $keys = self::getKeysToUsedInBothArrays($data2[0], $data1[0]);
        if (count($keys) === 0) {
            throw new \Exception('No keys matched between arrays given');
        }
        $returnArray=[];
        // do for the remaining data rows
        for($i = 0; $i < count($data2); $i++) {
            if (!self::checkDataRow($data1, $data2[$i], $keys)) {
                if(!$findAll){
                    return $data2[$i];
                }
                $returnArray[]=$data2[$i];
            }
        }
        return $returnArray;
    }

    /**
     * note: only use simple associated arrays, not multidimensional arrays
     * @param array $data
     * @param array $data2
     * @return array
     */
    static function getKeysToUsedInBothArrays(array $data, array $data2): array
    {
        $data2Keys = array_keys($data2);
        $count = count($data2Keys);
        for ($i = 0; $i < $count; $i++) {
            if (!array_key_exists($data2Keys[$i], $data)) {
                unset($data2Keys[$i]);
            }
        }
        return $data2Keys;
    }

    /**
     * @param array $csvData
     * @param array $dataRow
     * @param array $keys
     * @return bool
     */
    static function checkDataRow(array $csvData, array $dataRow, array $keys) : bool
    {
        $count = count($csvData);
        for ($i = 0; $i < $count; $i++) {
            $result = self::checkAgainstCsvRow( $dataRow, $csvData[$i], $keys);
            if($result === true){
                return true;
            }
        }
        return false;
    }

    /**
     * @param array $dataRow
     * @param array $csvDataRow
     * @param array $keys
     * @return bool
     */
    static function checkAgainstCsvRow(array $dataRow, array $csvDataRow, array $keys): bool
    {
        foreach ($keys as $singleKey) {
            if ($dataRow[$singleKey] != $csvDataRow[$singleKey]) {
                return false;
            }
        }
        return true;
    }

}