# Goals

* Create a Spark Scala program that turns images into training data for foreground-background classification.

# Problem Details 

*  In this assignment, I creatied a parallel program in Spark (or optionally MapReduce) that produces labeled training data from images.

*  Given several image files from high-resolution brain scans, we have to classify each pixel as foreground (belongs to an axon) or background (does not belong to an axon). 

* Although the image is noisy, axons (the lines going across the image) are clearly visible. To improve the quality of algorithms that automatically trace these axons in an image, we want to classify each pixel as foreground (belongs to an axon) or background (does not belong to an axon).

* We cannot simply look at the brightness of a pixel to make this decision, though a threshold on brightness might be accurate in most cases. Some noise or isolated “blots” can be brighter than some pixels belonging to an axon. Hence the decision should be made based on the neighborhood, e.g., a
human expert would look for bright pixels forming a line.

* The ultimate goal is to automate this task using machine learning.

* Expert-provided labels that identify foreground and background in a given image are present in the corresponding *_dist_* files.

* Each training record for some pixel (i, j, k) in an image should consist of 
	* the neighborhood vector centered around (i, j, k), and
	* the foreground/background label of (i, j, k). The former will be extracted from the image file, the latter from the distance file.

# Class Label
* We can derive the label of the center pixel (i, j, k) from the corresponding *_dist.tiff file. It has the same layered format as the image file, but contains “brightness” values that store for each pixel pixel (i, j, k) its distance from the nearest expert-identified foreground pixel. The final label of (i, j, k) should be set as follows:
	* If the distance value is 0 or 1, set it to “foreground”. (You can represent class “foreground” by a
      number, e.g., 1.)
	* If the distance value is greater than 3, set it to “background”. (You can represent class “background” by a number, e.g., 0.)
	* For all other distance values, i.e., 2 and 3, the class is unknown. Such records should not be used
	  for training or testing. Hence you can discard them.

#Putting It All Together
1.Using the above information, you can conceptually generate training data as follows:
	1. Select a pixel (i, j, k) from the image file.
	2. Extract the label of (i, j, k)—foreground or background, or discard it if unknown—from the
		distance file.
	3. Extract the neighborhood of size x-by-y-by-z from the image file and represent it as a vector of
		size x*y*z.
	4. Output vector and label together as a training record.

# File Structure 

* LoadMultiStack is used to read the images and returns the entire image in a data structure, so that we can access it in classifier to classify the pixels as background or foreground.

* classifier is the actual implementation of foreground-background classification in Spark.

* I wrote a brief report about my findings in report.pdf.

## Author
* **Shubham Deb**
