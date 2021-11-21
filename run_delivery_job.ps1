$CURRENT_DATE = GET-DATE -Format "yyyy/dd/MM"
mvn clean package "-Dmaven.test.skip=true";

$JAR_PATH = Resolve-Path ./target/linkedin-batch-*-*-0.0.1-SNAPSHOT.jar
java -jar "-Dspring.batch.job.names=deliverPackageJob" $JAR_PATH "item=bottles" "run.date(date)=$CURRENT_DATE"
pause;