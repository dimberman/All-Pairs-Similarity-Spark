# All-Pairs-Similarity-Spark


All-Pairs-Similarity-Search is an important function for clustering similar objects within large datasets. APSS is used in a wide variety of important use-cases such as:

- recommendation systems (e.g. Netflix finding users similar to you so they can recommend movies)
- fraud/plagiarism detection (finding near-duplicate essays)
- spam detection (e.g. google comparing every email recieved by a user to known spam-emails)

# Building


This package is set up with sbt and assembly. Run sbt clean assembly to create a jar in the target/scala folder. Requires Spark 1.5.2.


#running

The main spark command is:
spark-submit --class edu.ucsb.apss.Main apss-1.0.jar 

The package comes with the following options


| short option |      long option      |                               description |
|----|:-----------------:|---------------------------------------------:|
| -i |      --input      |                               The input file |
| -t |    --threshold    |       The threshold for PSS, defaults to 0.9 |
| -n |    --numLayers    |     Number of layers for PSS. Defaults to 21 |
| -h | --histogram-title | Title for histogram, defaults to "histogram" |


#sources

Based on the following papers

- X. Tang, X. Jin, T. Yang. Cache-Conscious Runtime Optimization for Ranking Ensembles. Proc. of 2014 ACM SIGIR conference on Research and Development in Information Retrieval. Slides.
- X. Tang, M. Alabduljalil, X. Jin, T. Yang, Load Balancing for Partition-based Similarity Search. Proceedings of 2014 ACM SIGIR conference on Research and Development in Information Retrieval. Slides.
- Maha Alabduljalil, Xun Tang, Tao Yang, Optimizing Parallel Algorithms for All Pairs Similarity Search. WSDM'2013 (6th ACM International Conference on Web Search and Data Mining. Finalist for the best student paper award. Slides.
