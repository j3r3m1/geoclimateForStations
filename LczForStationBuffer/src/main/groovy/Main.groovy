// Add as comment in IntelliJ
@GrabResolver(name='orbisgis', root='https://oss.sonatype.org/content/repositories/snapshots/')
@Grab(group='org.orbisgis.geoclimate', module='geoclimate', version='1.0.1-SNAPSHOT')

// Remove comment in IntelliJ
// package org.orbisgis.geoclimate.geoindicators

import org.orbisgis.data.jdbc.JdbcDataSource
import static org.orbisgis.data.H2GIS.open
import org.orbisgis.geoclimate.Geoindicators

// Path of the file containing the buffer geometries around stations
File buffer_dir = new File(args[0])

// Path where the resulting files with LCZ type and indicators for each station buffer will be saved
String outputPath = args[1]

// Directory where are saved the GeoClimate layers
String geoclimate_dir = args[2]

// Name of the file format extension (for GeoClimate output files)
String extension = ".fgb"

// Name of the ID station column (the id should be an integer)
String ID_station = args[3]

// Name of the dataset
String dataset = args[4]


// Modify initial input parameters (set only LCZ calculation)
Map input_params = Geoindicators.WorkflowGeoIndicators.getParameters()
input_params["indicatorUse"] = ["LCZ", "TEB", "UTRF"]

// Open an H2GIS connection
h2GIS = open(File.createTempDir().toString() + File.separator + "myH2GIS_DB;AUTO_SERVER=TRUE")

main(geoclimate_dir, h2GIS, input_params, buffer_dir, outputPath, ID_station, extension, dataset)

static void main(String inputDir, JdbcDataSource h2GIS, Map input_params, File buffer_dir,
                 String outputPath, String ID_station, String extension, String dataset) {
  String outputLcz = "RSU_LCZ"
  String outputIndic = "RSU_INDICATORS"
  
  String extension_tmp
  // Load GeoClimate files into the Database
  for (l in ["zone", "rail", "road", "building", "vegetation",
             "water", "impervious", "sea_land_mask", "urban_areas",
             "building_height_missing"]){
    if (l == "building_height_missing"){
      extension_tmp = ".csv"
    }
    else{
      extension_tmp = extension
    }
    File filename = new File(inputDir + File.separator + l + extension_tmp)
    if (filename.exists() and filename.length() != 0){
      h2GIS.load(filename.toString(), l)
    }
    else{
      println(filename.toString() + " do not exist or is empty")
    }
  }
  // Load the RSU (which are the buffer around station) into the database
  if (buffer_dir.exists()){
    h2GIS.load(buffer_dir.toString(), "rsu")
    h2GIS """ALTER TABLE RSU RENAME COLUMN $ID_station TO id_rsu"""
  }
  else{
    println(buffer_dir.toString() + " do not exist")
  }

  // Create a table where to append all results at the end
  List queryFinLcz = []
  List queryFinIndic = []
  Map rsu_list = [:]

  // Run the calculation for each unit (buffer circle around station)
  int i = 0
  int n = h2GIS.firstRow ("SELECT COUNT(*) AS n FROM rsu")["n"]
  h2GIS.eachRow("SELECT * FROM rsu") {row ->
    i++
    def rowMap = row.toRowResult()
    def id_rsu = rowMap."ID_RSU"

    println("Calculate station $id_rsu ($i/$n)")

    println("1")
    // Need to delete the potential existing id_rsu in the building table
    if(dataset == "osm"){
        h2GIS """DROP TABLE IF EXISTS building_tempo;
                CREATE TABLE building_tempo
                    AS SELECT * EXCEPT(ID_RSU, ID_BLOCK)
                    FROM building"""
    }
    else{
        h2GIS """DROP TABLE IF EXISTS building_tempo;
                CREATE TABLE building_tempo
                    AS SELECT * EXCEPT(ID_RSU)
                    FROM building"""
    }
    println("2")
    // Create a table containing a single unit...
    h2GIS """DROP TABLE IF EXISTS RSU$id_rsu;
                    CREATE TABLE RSU$id_rsu
                      AS SELECT * FROM RSU
                      WHERE ID_RSU = $id_rsu"""
    println("3")                 
    // Restrict the zone to the buffer
    h2GIS """DROP TABLE IF EXISTS zone_tempo;
            CREATE TABLE zone_tempo
                AS SELECT the_geom as the_geom, 'rsu$id_rsu' as ID_RSU
                FROM rsu$id_rsu"""
    println("4")
    rsu_list[id_rsu] = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(
                               h2GIS,
                              "zone_tempo",
                              "building_tempo",
                              "road",
                              "rail",
                              "vegetation",
                              "water",
                              "impervious",
                              "building_height_missing",
                              "sea_land_mask",
                              "urban_areas",
                              "RSU$id_rsu",
                              input_params,
                              "rsu$id_rsu")
    println("5")
    // Add the table to the list of tables to union
    queryFinLcz.add("SELECT * FROM ${rsu_list[id_rsu]['rsu_lcz']}")
    queryFinIndic.add("SELECT * FROM ${rsu_list[id_rsu]['rsu_indicators']}")

  }
  // Union all buffers for LCZ and indicators
  h2GIS """DROP TABLE IF EXISTS $outputLcz;
                      CREATE TABLE $outputLcz
                      AS ${queryFinLcz.join(" UNION ALL ")};
                ALTER TABLE $outputLcz RENAME COLUMN id_rsu TO $ID_station"""
  h2GIS """DROP TABLE IF EXISTS $outputIndic;
                    CREATE TABLE $outputIndic
                    AS ${queryFinIndic.join(" UNION ALL ")};
            ALTER TABLE $outputIndic RENAME COLUMN id_rsu TO $ID_station"""
  println("6")
  // Save results
  h2GIS.save(outputLcz, outputPath + File.separator + "${outputLcz}.fgb")
  h2GIS.save(outputIndic, outputPath + File.separator + "${outputIndic}.fgb")
}