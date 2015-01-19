<?php
    $ini_array = parse_ini_file("config/config.ini", true);
    $key = $ini_array['map']['key'];
?>            

            
<script type="text/javascript" src="https://maps.googleapis.com/maps/api/js?key=<?php echo $key ?>"></script>
<script src="bootstrap/js/heatmap.js"></script>
<script src="bootstrap/js/gmaps-heatmap.js"></script>
<script type="text/javascript">
// map center
        var myLatlng = new google.maps.LatLng(41.8500300,-87.6500500);
        // map options,
        var myOptions = {
          zoom: 12,
          center: myLatlng

        };
        // standard map
        map = new google.maps.Map(document.getElementById("map-canvas"), myOptions);
        // heatmap layer
        heatmap = new HeatmapOverlay(map, 
          {
            // radius should be small ONLY if scaleRadius is true (or small radius is intended)
            "radius": 0.004,
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
          data: [{lat: 41.8500300, lng:-87.6500500, count: 2}, {lat: 41.8500300, lng:-87.6600500, count: 3},{lat: 41.8600300, lng:-87.6600500, count: 4}]
        };

        heatmap.setData(testData);

</script>