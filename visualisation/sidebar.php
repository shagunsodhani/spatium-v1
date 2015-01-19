<div class="col-sm-3 col-md-2 sidebar">
    <ul class="nav nav-sidebar">
        <li><a href = "">CLEAR MAP</a></li>
    
    <?php
        include 'db.php';
        $conn = connect();
        $sql = "SELECT DISTINCT primary_type FROM `dataset`";
        $result = query($conn, $sql);
        $count = 1;
        while($row = $result->fetch(PDO::FETCH_ASSOC)) 
        {
            echo "<li><a href=\"".$count.".html\">".$row['primary_type']."</a></li>\n";
            $count++;
        }
    ?>
    </ul>
</div>