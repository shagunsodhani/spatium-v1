<?php
error_reporting(E_ALL | E_STRICT);
ini_set('display_errors',1);
ini_set('html_errors', 1);

function connect()
{
    $ini_array = parse_ini_file("config/config.ini", true);
    $host = $ini_array['mysql']['host'];
    $user = $ini_array['mysql']['user'];
    $passwd = $ini_array['mysql']['passwd'];
    $db = $ini_array['mysql']['db'];
    try 
    {
        $conn = new PDO("mysql:host=$host;dbname=$db", $user, $passwd);
        // set the PDO error mode to exception
        $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        // echo "Connected successfully\n"; 
        return $conn;
    }
    catch(PDOException $e)
    {
        echo "Connection failed: " . $e->getMessage() . "\n";
        return -1;
    }
}

function query($conn, $sql)
{
    try 
    {
        return $conn->query($sql); 
    } 
    catch(PDOException $e) 
    {
        echo "An Error occured: " . $e->getMessage(). "\n";
        some_logging_function($ex->getMessage());
        return -1;
    }
}

?>