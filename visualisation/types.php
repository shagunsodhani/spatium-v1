<ul class="nav nav-sidebar">
<?php
    require_once('db.php');
    $conn = connect();
    $sql = "SELECT DISTINCT primary_type FROM `dataset`";
    $result = query($conn, $sql);
    $conn = null;
    while($row = $result->fetch(PDO::FETCH_ASSOC)) 
    {
        echo "<li><a href=\"index.php?type=".$row['primary_type']."&param=".$param."\">".$row['primary_type']."</a></li>\n";
    }
?>
</ul>
