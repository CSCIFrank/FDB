JAVAC=javac
JAVA=java
CLASSPATH=.:lib/*
OUTDIR=out

MODELDIR=src/**/models
SOURCEDIR=src
ITDIR=src/**/iterators


sources = $(wildcard $(SOURCEDIR)/**/StatusCode.java $(SOURCEDIR)/**/DBConf.java $(MODELDIR)/AssignmentOperator.java $(MODELDIR)/AttributeType.java $(MODELDIR)/AlgebraicOperator.java $(MODELDIR)/AlgebraicExpression.java $(MODELDIR)/AssignmentExpression.java $(MODELDIR)/ComparisonOperator.java $(SOURCEDIR)/**/utils/ComparisonUtils.java $(MODELDIR)/ComparisonPredicate.java $(MODELDIR)/IndexType.java $(MODELDIR)/IndexRecord.java $(MODELDIR)/NonClusteredBPTreeIndexRecord.java  $(MODELDIR)/NonClusteredHashIndexRecord.java $(MODELDIR)/Record.java $(MODELDIR)/TableMetadata.java $(SOURCEDIR)/**/fdb/FDBKVPair.java $(SOURCEDIR)/**/fdb/FDBHelper.java $(SOURCEDIR)/**/TableMetadataTransformer.java $(SOURCEDIR)/**/TableManager.java $(SOURCEDIR)/**/TableManagerImpl.java $(SOURCEDIR)/**/RecordsTransformer.java $(SOURCEDIR)/**/utils/IndexesUtils.java $(SOURCEDIR)/**/IndexTransformer.java $(SOURCEDIR)/**/Cursor.java $(SOURCEDIR)/**/Records.java $(SOURCEDIR)/**/RecordsImpl.java $(SOURCEDIR)/**/Indexes.java $(SOURCEDIR)/**/IndexesImpl.java $(SOURCEDIR)/**/Iterator.java $(SOURCEDIR)/**/RelationalAlgebraOperators.java $(ITDIR)/selectIterator.java $(ITDIR)/projectIterator.java $(ITDIR)/joinIterator.java $(SOURCEDIR)/**/RelationalAlgebraOperatorsImpl.java $(SOURCEDIR)/**/test/*.java)

classes = $(sources:.java=.class)

preparation: clean
	mkdir -p ${OUTDIR}

clean:
	rm -rf ${OUTDIR}

%.class: %.java
	$(JAVAC) -d "$(OUTDIR)" -cp "$(OUTDIR):$(CLASSPATH)" $<

part1Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore FDBProject.test.Part1Test

part2Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore FDBProject.test.Part2Test

part3Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore FDBProject.test.Part3Test

part4Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore FDBProject.test.Part4Test


.PHONY: part1Test part2Test clean preparation