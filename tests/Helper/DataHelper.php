<?php

namespace PaulMillband\SqlLibrary\tests\Helper;

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
        $keys = self::getKeysToUse($data2[0], $data1);
        $return=[];
        // do for the remaining data rows
        for($i = 0; $i < count($data2); $i++) {
            if (!self::checkDataRow($data1, $data2[$i], $keys)) {
                if(!$findAll){
                    return $data2[$i];
                }
                $return[]=$data2[$i];
            }
        }
        return $return;
    }

    static function checkDataRow(array $csvData, array $dataRow, array $keys): bool
    {
        if ( !self::checkDataRowAgainstCsv($dataRow, $csvData, $keys) ) {
            return false;
        }
        return true;
    }

    static function getKeysToUse(array $data, array $csvData): array
    {
        $csvKeys = array_keys($csvData[0]);
        $count = count($csvKeys);
        for ($i = 0; $i < $count; $i++) {
            if (!array_key_exists($csvKeys[$i], $data)) {
                unset($csvKeys[$i]);
            }
        }
        return $csvKeys;
    }

    static function checkDataRowAgainstCsv(array $dataRow, array $csvData, array $keys)
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