install: 
	@./gradlew install
update:
	@./gradlew install --refresh-dependencies
test:
	@./gradlew test --tests Tester
adhoc:
	@./gradlew test --tests Adhoc
idea: 
	@./gradlew idea
doc:
	@./gradlew javadoc
clean: 
	@./gradlew clean 
