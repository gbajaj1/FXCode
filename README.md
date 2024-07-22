An Azure Function implementation for an HTTP triggered function that splits a file (Blob) into multiple Blobs based on the information present inside it.

The input file name is received in the JSON message (input). The input file is stored as a blob in the attached storage. 

The output files are stored in the same blob container. 
