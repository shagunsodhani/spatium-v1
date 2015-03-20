<?php
    include 'header.php';
    $type = "ARSON";
    $param = "home";
    if(isset($_GET['type']))
        $type = $_GET['type'];
    if(isset($_GET['param']))
        $param = $_GET['param'];
    echo $param;
    if ($param == "home")
    {
        include 'home.php';
    }
    elseif ($param == "growth") {
        include 'growth.php';
    }
    elseif ($param == "typeplot") {
        include 'typeplot.php';
    }
    elseif ($param == "timeseries") {
        include 'timeseries.php';
    }
    elseif ($param == "stream") {
        include 'streamdata.php';
    }
    elseif ($param == "outlier") {
        include 'outlier.php';
    }


?>
