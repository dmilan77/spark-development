`sbt clean build`


`spark-submit --packages com.databricks:spark-csv_2.10:1.5.0  --class "CreateCSVFileFromHive" --master local /tmp/spark-development_2.10-1.0.jar`
