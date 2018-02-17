# Goals

* Implement PageRank in Spark to compare the behavior of an iterative graph algorithm in Spark vs. Hadoop MapReduce.

# Pagerank 

* PageRank works by counting the number and quality of links to a page to determine a rough estimate of how important the website is. The underlying assumption is that more important websites are likely to receive more links from other websites.

* Here I am analysing and implementing iterative algorithm like Pagerank in Hadoop in order to find the most referenced Wikipedia articles.

[White Paper from Brin & Page](http://infolab.stanford.edu/~backrub/google.html). We describe the web according to the model defined by Brin & Page.

	* Web = oriented graph with n nodes (pages) and branches (links)
	* Web surfer = goes from nodes to nodes through links or teleportation (dumping factor)

* When the web surfer is on a node "i" * [proba (1-d)] goes randomly on linked nodes (j_1,... j_k) through n_i outcoming links * [proba d] goes randomly among the n nodes

# Algorithm

* The PageRank algorithm outputs a probability distribution used to represent the likelihood that a person randomly clicking on links will arrive at any particular page. PageRank can be calculated for collections of documents of any size. It is assumed in several research papers that the distribution is evenly divided among all documents in the collection at the beginning of the computational process. The PageRank computations require several passes, called “iterations”, through the collection to adjust approximate PageRank values to more closely reflect the theoretical true value.

* Preprocessed the original Wikipedia 2006 data dump into Hadoop-friendly bz2-compressed files and then used SAX-XML parser to keep relevant links from a Wikipedia page.

* Seperated the workflow of the program into 3 jobs:
	* Pre-processing Job: Converted input Wikipedia data into a graph represented as adjacency lists. 
	* PageRank Job: ran 10 iterations of PageRank.
	* Top-k Job: From the output of the last PageRank iteration, filtered out the 100 pages with the highest PageRank and output them, along with their ranks, from highest to lowest. 

* Ran your program in Elastic MapReduce (EMR) on the four provided bz2 files, which comprise the full English Wikipedia data set from 2006, using the following two configurations:
	* 6 m4.large machines (1 master and 5 workers)
	* 11 m4.large machines (1 master and 10 workers)

# File Structure 

* bz2WikiParser is a SAX-XML parser program to keep relevant links from a Wikipedia page.

* pagerank is the actual implementation of Pagerank in Spark.

* I wrote a brief report about my findings in report.odt.

## Author
* **Shubham Deb**
