<div class="col-sm-3 col-md-2 sidebar">
    <ul class="nav nav-sidebar">
        <li><a href = "index.php?type=clear">CLEAR MAP</a></li>
    
    <?php

        require_once('db.php');
        $conn = connect();
        $sql = "SELECT DISTINCT primary_type FROM `dataset`";
        $result = query($conn, $sql);
        $conn = null;
        $count = 1;
        while($row = $result->fetch(PDO::FETCH_ASSOC)) 
        {
            echo "<li><a href=\"index.php?type=".$row['primary_type']."\">".$row['primary_type']."</a></li>\n";
            $count++;
        }
    ?>
    </ul>
</div>