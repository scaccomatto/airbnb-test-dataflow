# airbnb-test-dataflow


## CREATE DATAFLOW TEMPLATE
```
 mvn compile exec:java -Dexec.mainClass=com.amido.AirbnbTemplate -Dexec.args="--runner=DataflowRunner --project=bigqueryteo-238514 --stagingLocation=gs://demo_aribnb_londra/staging --templateLocation=gs://demo_aribnb_londra/templates/AirbnbTemplate"


 ```