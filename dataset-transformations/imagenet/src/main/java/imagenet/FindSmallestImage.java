import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.awt.image.Raster;

public class FindSmallestImage {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: FindSmallestImage <proc-id> <nprocs> <input-dir>");
            System.exit(1);
        }

        final int pid = Integer.parseInt(args[0]);
        final int nprocs = Integer.parseInt(args[1]);
        final String inputDir = args[2];

        final File inputDirFile = new File(inputDir);
        String[] inputFilenames = inputDirFile.list();

        int minWidth = Integer.MAX_VALUE;
        String minWidthImage = null;
        int minHeight = Integer.MAX_VALUE;
        String minHeightImage = null;

        for (int i = 0; i < inputFilenames.length; i++) {
            if (i % nprocs != pid) {
                continue;
            }

            final String inputPath = inputDir + "/" + inputFilenames[i];
            assert(inputPath.endsWith(".JPEG"));

            // System.err.println((i + 1) + "/" + inputFilenames.length);

            final BufferedImage img;
            try {
                img = ImageIO.read(new File(inputPath));
            } catch (Exception e) {
                System.err.println("Error on file " + inputPath);
                e.printStackTrace();
                continue;
            }

            final Raster raster = img.getData();
            final int width = raster.getWidth();
            final int height = raster.getHeight();

            if (width < minWidth) {
                minWidth = width;
                minWidthImage = inputPath;
            }

            if (height < minHeight) {
                minHeight = height;
                minHeightImage = inputPath;
            }
        }

        System.out.println("minWidth=" + minWidth + " (" + minWidthImage +
                "), minHeight=" + minHeight + " (" + minHeightImage + ")");
    }
}

