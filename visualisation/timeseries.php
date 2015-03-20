<?php
    
    require_once("db.php");
    $type = "ARSON";
    if(isset($_GET['type']))
        $type = $_GET['type'];
    include 'header.php';
?>
    <body>
        <nav class="navbar navbar-inverse navbar-fixed-top" role="navigation">
            <div class="container-fluid">
            <!-- Brand and toggle get grouped for better mobile display -->
                <div class="navbar-header">
                    <a class="navbar-brand" href="index.php">Spatium</a>
                </div>
                <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                    <ul class="nav navbar-nav">
                        <li class="active"><a href="#">Data Summary<span class="sr-only">(current)</span></a></li>
                        <li><a href="#">Link</a></li>
                    </ul>
                    <ul class="nav navbar-nav navbar-right">
                        <li><a href="https://github.com/shagunsodhani/spatium">Code</a></li>
                    </ul>
                </div><!-- /.navbar-collapse -->
            </div><!-- /.container-fluid -->
        </nav>

        <div class="container-fluid">
            <div class="row">
                <?php 
                include 'sidebar.php';
                ?>
                <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
                    <div class="container" style="max-width: 800px; height: 400px; margin: 0 auto">
                    </div>
                </div>
            </div>
        </div>
        <script src="bootstrap/js/jquery-1.11.2.min.js"></script>
        <script src="bootstrap/js/bootstrap.min.js"></script>
        <script src="bootstrap/js/highcharts.js"></script>
        <script src="bootstrap/js/highcharts_modules.js"></script>
        <script src="bootstrap/js/data_summary.js"></script>
        <script type="text/javascript"> 
            $(document).ready(function () 
            {
                type_occurence_static();
            });
        </script>

    <!-- </body>
</html>
