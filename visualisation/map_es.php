<?php
    
    
    require '../vendor/autoload.php';

    $ini_array = parse_ini_file("config/config.ini", true);
    $key = $ini_array['map']['key'];

    $params = array();
    $params['hosts'] = array ('192.168.111.180:9200'); 

    // echo "print1";  
    $client = new Elasticsearch\Client($params);

    $geotools = new \League\Geotools\Geotools();
    
    // $type = "THEFT";    
    // $size = 30;
    // $precision = 6;
    // echo $type;
    // echo $precision;
    // echo (int)$precision;
    $params = array();
    $params['index'] = 'titan';
    $params['body']['aggs']['aggs1']['filter']['query']['match']['1l1'] = $type;
    $params['body']['aggs']['aggs1']['aggs']['aggs2']['geohash_grid']['field'] = '35x';
    $params['body']['aggs']['aggs1']['aggs']['aggs2']['geohash_grid']['precision'] = (int)$precision;
    $params['body']['size'] = 0;
        
    $result = $client->search($params); 
    $a = $result['aggregations']['aggs1']['aggs2']['buckets'];
    // $decoded = $geotools->geohash()->decode($location);

    // foreach ($a as $key=>$value)
    // {
    //     $location = $value['key'];
    //     $count = $value['doc_count'];
    //     $decoded = $geotools->geohash()->decode($location);
    //     $lat = $decoded->getCoordinate()->getLatitude();
    //     $lng = $decoded->getCoordinate()->getLongitude();
    //     echo $count." ".$location." ".$lat." ".$lng."\n";
    // }

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
        zoom: 11,
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

        $count = count($a);
        // print $count;
        // print_r($a);
        if ($count>0)
        {
            for($i = 0; $i<$count-1; $i++)  
            {
                $value = $a[$i];
                $location = $value['key'];
                $decoded = $geotools->geohash()->decode($location);
                $lat = $decoded->getCoordinate()->getLatitude();
                $lng = $decoded->getCoordinate()->getLongitude();
                $counter = $value['doc_count'];

                echo " {lat: ".$lat.", ";
                echo "lng: ".$lng.", ";
                echo "count: ".$counter."},";
                // echo "count: 1}, ";
            }
            $value = $a[$count-1];

            $location = $value['key'];
            $decoded = $geotools->geohash()->decode($location);
            $lat = $decoded->getCoordinate()->getLatitude();
            $lng = $decoded->getCoordinate()->getLongitude();
            $counter = $value['doc_count'];

            echo " {lat: ".$lat.", ";
            echo "lng: ".$lng.", ";
            echo "count: ".$counter."}";
        }
    ?> 
    ]};
    heatmap.setData(testData);

</script>
