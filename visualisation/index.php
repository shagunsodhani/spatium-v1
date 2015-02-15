<?php
    
    require_once("db.php");
    $type = "clear";
    $size = 1;
    if(isset($_GET['type']))
        $type = $_GET['type'];
    if(isset($_GET['size']))
        $size = $_GET['size'];

?>
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport/" content="width=device-width, initial-scale=1">
        <title>Spatium</title>
        <link href="bootstrap/css/bootstrap.min.css" rel="stylesheet">
        <link href="bootstrap/css/spatium.css" rel="stylesheet">
        <style type="text/css">
            html, body, .container-fluid, .row, .main, #map-canvas { height: 100%; margin-left: 0; padding-left: 5%;}
        </style>

    </head>
    
    <body>
        <nav class="navbar navbar-inverse navbar-fixed-top" role="navigation">
            <div class="container-fluid">
                <div class="navbar-header">
                    <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                    </button>
                    <a class="navbar-brand" href="index.php">Spatium</a>
                </div>
                <div class="collapse navbar-collapse navbar-right" id="bs-example-navbar-collapse-1">
                    <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                    </button>
                    <a class="navbar-brand" href="https://github.com/shagunsodhani/spatium">Github</a>
                </div>
            </div>
        </nav>
        <div class="container-fluid">
            <div class="row">
                <?php include 'sidebar.php';
                ?>
                <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
                    <h4 class="page-header">
		    	        <div class="btn-group">
		    	            <button type="button" class="btn btn-info dropdown-toggle" data-toggle="dropdown" aria-expanded="false">
    			                Size of Co-location <span class="caret"></span>
  			                </button>
  			                <ul class="dropdown-menu" role="menu">
    			                <li><a href="index.php?type=<?php echo $type?>&size=1">1</a></li>
    			                <li><a href="index.php?type=<?php echo $type?>&size=2">2</a></li>
                                <li><a href="index.php?type=<?php echo $type?>&size=3">3</a></li>
  			                </ul>
		                </div>
		            </h4>
                    <div class="row placeholders">
                        <div class="col-sm-12" id="map-canvas" >
                            <h4>Label</h4>
                            <span class="text-muted">Something else</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <?php
            include "map_es.php";
        ?>
        <script src="bootstrap/js/jquery-1.11.2.min.js"></script>
        <script src="bootstrap/js/bootstrap.min.js"></script>
    </body>
</html>
