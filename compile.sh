
### convert file "TypeDefinition.json" to "TypeDefinition.java"

# python ./src/convert.py

### compile src files into JAR

cd src
hadoop com.sun.tools.javac.Main *.java ./org/json/*.java
jar cf logprocess.jar *.class ./org/json/*.class
mv logprocess.jar ../
cd ..

