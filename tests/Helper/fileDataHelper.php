<?php

namespace PaulMillband\SqlLibrary\tests\Helper;

class fileDataHelper
{
    static function isComparativeData($file, $data): bool
    {
        $csvData = self::getDataFromCsv($file, $data);
        $dataRow = $data->fetch_array();
        $keys = self::getKeysToUse($dataRow, $csvData);
        if (!self::checkDataRow($csvData, $dataRow, $keys)) {
            return false;
        }
        while ($dataRow = $data->fetch_array()) {
            if (!self::checkDataRow($csvData, $dataRow, $keys)) {
                return false;
            }
        }
        return true;
    }

    static function getDataFromCsv(string $filePath): array
    {
        $data = array_map(fn($v) => str_getcsv($v, separator: ',', escape: ''), file($filePath));
        array_walk($data, function (&$a) use ($data) {
            $a = array_combine($data[0], $a);
        });
        array_shift($data); # remove column header
        return $data;
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