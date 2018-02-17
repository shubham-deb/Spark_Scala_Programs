import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoadMultiStack
{
    private static final Logger logger = LogManager.getLogger(LoadMultiStack.class);

    public static void main(final String args[]) {
    	if (args.length != 4) {
    		logger.error("Usage:\njava LoadStack <stack filename> <xdim> <ydim> <zdim>");
			System.exit(1);
    	}

		try {
			final File imageFile = checkFile(args[0]);
			final int xDim = Integer.parseInt(args[1]);
			final int yDim = Integer.parseInt(args[2]);
			final int zDim = Integer.parseInt(args[3]);
			final ImageReader reader = buildReader(imageFile, zDim);
	        final byte imageBytes[] = new byte[xDim * yDim * zDim];

			for (int ix = 0; ix < zDim; ix++) {
				final BufferedImage image = reader.read(ix);
		        final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
//				System.out.println(layerBytes.length + "    " + xDim * yDim);
				System.arraycopy(layerBytes, 0, imageBytes, ix * xDim * yDim, xDim * yDim);
			}

	        for (int iz = 0 ; iz < zDim ; iz++) {
		        for (int iy = 0 ; iy < yDim ; iy++) {
			        for (int ix = 0 ; ix < xDim ; ix++) {
//			        	System.out.println(imageBytes[iz * yDim * xDim + iy * xDim + ix]);
			        }
		        }
	        }

		} catch (final Exception e) {
			logger.error("", e);
			System.exit(1);
		}
	}

    private static File checkFile(final String fileName) throws Exception {
    	final File imageFile = new File(fileName);
    	if (!imageFile.exists() || imageFile.isDirectory()) {
    		throw new Exception ("Image file does not exist: " + fileName);
    	}
    	return imageFile;
    }

    private static ImageReader buildReader(final File imageFile, final int zDim) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();
		iter = null;
		reader.setInput(imgInStream);
		int numPages;
		if ((numPages = reader.getNumImages(true)) != zDim) {
			throw new Exception("Data load error - Number of pages mismatch: " + numPages + " expected: " + zDim);
		}
		return reader;
    }
}