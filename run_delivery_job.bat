mvn clean package -Dmaven.test.skip=true;
java -jar ./target/linkedin-batch-*-*-0.0.1-SNAPSHOT.jar "item=shoes" "run.date(date)=%DATE%";
read;
