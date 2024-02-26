# Read CSV File into a PySpark DataFrame

```
# Using csv("path") of DataFrameReader
df = spark.read.csv("<FILE PATH>")

# Using format("csv").load("path") of DataFrameReader
df = spark.read.format("csv").load("<FILE PATH>")

# Read Multiple CSV Files
df = spark.read.csv("<FILE PATH_1>, <FILE PATH_2> ... <FILE PATH_N>")

# Read all CSV Files in a Directory
df = spark.read.csv("<FOLDER_PATH>")

Note: PySpark reads all columns as a string (StringType) by default.

# If you have a header with column names on your input file
df = spark.read.format("csv").option("header",True).load("<FILE PATH>") 

# If you have to specify the column delimiter of the CSV file. By default, it is comma (,) character.
df = spark.read.format("csv").options("delimiter",'|').load("<FILE PATH>")

# If you have to automatically infers column types based on the data.
df = spark.read.format("csv").options("inferSchema",True).load("<FILE PATH>") 
```

You can set the following CSV-specific options to deal with CSV files:

- sep (default ,): sets the single character as a separator for each field and value.
- encoding (default UTF-8): decodes the CSV files by the given encoding type.
- quote (default "): sets the single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behaviour is different form com.databricks.spark.csv.
- escape (default ): sets the single character used for escaping quotes inside an already quoted value.
- comment (default empty string): sets the single character used for skipping lines beginning with this character. By default, it is disabled.
- header (default false): uses the first line as names of columns.
- inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
- ignoreLeadingWhiteSpace (default false): defines whether or not leading whitespaces from values being read should be skipped.
- ignoreTrailingWhiteSpace (default false): defines whether or not trailing whitespaces from values being read should be skipped.
- nullValue (default empty string): sets the string representation of a null value.
- nanValue (default NaN): sets the string representation of a non-number" value.
- positiveInf (default Inf): sets the string representation of a positive infinity value.
- negativeInf (default -Inf): sets the string representation of a negative infinity value.
- dateFormat (default null): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to both date type and timestamp type. By default, it is null which means trying to parse times and date by java.sql.Timestamp.valueOf() and java.sql.Date.valueOf().
- maxColumns (default 20480): defines a hard limit of how many columns a record can have.
- maxCharsPerColumn (default 1000000): defines the maximum number of characters allowed for any given value being read.
- maxMalformedLogPerPartition (default 10): sets the maximum number of malformed rows Spark will log for each partition. Malformed records beyond this number will be ignored.
- mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
    - PERMISSIVE : sets other fields to null when it meets a corrupted record. When a schema is set by user, it sets null for extra fields.
    - DROPMALFORMED : ignores the whole corrupted records.
    - FAILFAST : throws an exception when it meets corrupted records.
