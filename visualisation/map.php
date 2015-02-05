<?php

    
    $ini_array = parse_ini_file("config/config.ini", true);
    $key = $ini_array['map']['key'];
    if($type!="CLEAR")
    {
    	$command = "/usr/lib/jvm/java-7-openjdk-amd64/bin/java -Xms4096M -Xmx7680M -Dfile.encoding=UTF-8 -classpath /home/precise/spatium/Titan/bin:/home/precise/spatium/Titan/lib/* Visualization ".$type;
	echo $command;
	echo exec($command);
    }

    $conn = connect();
    $sql = "SELECT ROUND(longitude,3) as lng, ROUND(latitude,3) as lat, count FROM `results`";
    // $sql = "SELECT ROUND(longitude,3) as lng, ROUND(latitude,3) as lat, COUNT(*) as count FROM `dataset` WHERE primary_type = :type GROUP BY lat, lng LIMIT 0, 1000";
    $sth = $conn->prepare($sql, array(PDO::ATTR_CURSOR => PDO::CURSOR_FWDONLY));
    $sth->execute(array(':type' => $type));
    $result = $sth->fetchAll();
    // $conn = null;

?>            

            
<script type="text/javascript" src="https://maps.googleapis.com/maps/api/js?key=<?php echo $key ?>"></script>
<script src="bootstrap/js/heatmap.js"></script>
<script src="bootstrap/js/gmaps-heatmap.js"></script>
<script type="text/javascript">
// map center
    var myLatlng = new google.maps.LatLng(41.8500300,-87.6500500);
    // map options,
    var myOptions = 
    {
        zoom: 10,
        center: myLatlng
    };
    // standard map
    map = new google.maps.Map(document.getElementById("map-canvas"), myOptions);
    // heatmap layer
    heatmap = new HeatmapOverlay(map, 
        {
            // radius should be small ONLY if scaleRadius is true (or small radius is intended)
            "radius": 0.008,
            "maxOpacity": 1, 
            // scales the radius based on map zoom
            "scaleRadius": true, 
            // if set to false the heatmap uses the global maximum for colorization
            // if activated: uses the data maximum within the current map boundaries 
            //   (there will always be a red spot with useLocalExtremas true)
            "useLocalExtrema": true,
            // which field name in your data represents the latitude - default "lat"
            latField: 'lat',
            // which field name in your data represents the longitude - default "lng"
            lngField: 'lng',
            // which field name in your data represents the data value - default "value"
            valueField: 'count',
            blur: 1,
            // gradient: 
            // {
            //   // enter n keys between 0 and 1 here
            //   // for gradient color customization
            //   '.5': 'green',
            //   '.8': 'blue',
            //   '.95': 'red'
            // }
        }
    );
    
    var testData = {
            max: 4,
            min: 2,
            data:[

    <?php

        $count = count($result);
        if ($count>0)
        {
            for($i = 0; $i<$count-1; $i++)  
            {
                $value = $result[$i];
                echo " {lat: ".$value['lat'].", ";
                echo "lng: ".$value['lng'].", ";
                echo "count: ".$value['count']."},";
                // echo "count: 1}, ";
            }
            $value = $result[$count-1];
            echo " {lat: ".$value['lat'].", ";
            echo "lng: ".$value['lng'].", ";
            echo "count: ".$value['count']."}";
        }
    ?> 
    ]};
    heatmap.setData(testData);

</script>
