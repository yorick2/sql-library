<?php

namespace PaulMillband\SqlLibrary\tests\Helper;


class CsvFileDataHelper
{
    /**
     * @param string $filePath
     * @param array $header array of column names, leave blank if header exists as first line in file
     * @return array an associated array for the csv data e.g. [ ["col1"=>"1","col2"=>"9"], ["col1"=>"4","col2"=>"6"] ]
    */
    static function getDataFromCsv(string $filePath, array $header=[], string $separator=','): array
    {
        if(!file_exists($filePath)||filetype($filePath)!='file') {
            throw new \Exception('file not found');
        }
        $fileContents = file($filePath);
        if($fileContents === false) {
            throw new \Exception('file not found');
        }
        $data = array_map(fn($v) => str_getcsv($v, separator: $separator, escape: ''), $fileContents);
        if(count($header)==0){
            $header = $data[0];
            array_shift($data);
        }
        array_walk($data, function (&$a) use ($data, $header) {
            $a = array_combine($header, $a);
        });
        return $data;
    }
}