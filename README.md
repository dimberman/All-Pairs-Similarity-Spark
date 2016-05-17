# All-Pairs-Similarity-Spark


All-Pairs-Similarity-Search is an important function for clustering similar objects within large datasets. APSS is used in a wide variety of important use-cases such as:

- recommendation systems (e.g. Netflix finding users similar to you so they can recommend movies)
- fraud/plagiarism detection (finding near-duplicate essays)
- spam detection (e.g. google comparing every email recieved by a user to known spam-emails)
- 

# Building


This package is set up with sbt and assembly. run sbt clean assembly to create a jar in the target/scala folder.


#running

The main spark command is:
spark-submit --class edu.ucsb.apss.Main apss-1.0.jar 

The package comes with the following options

  -i <value> | --input <value>
        input is the input file
  -t <value> | --threshold <value>
        threshold is the threshold for PSS, defaults to 0.9
  -n <value> | --numLayers <value>
        number of layers in PSS, defaults to 21
  -o <value> | --output <value>
        output directory for APSS, defaults to /user/output
  -h <value> | --histogram-title <value>
        title for histogram, defaults to "histogram"
  -d <value> | --debug <value>
        toggle debug logging. defaults to false
        
        
