import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.awt.image.Raster;

public class ConvertJPEGsToText {
    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("usage: ConvertJPEGsToText <process-id> " +
                    "<nprocesses> <min-height> <min-width> <input-dir> " +
                    "<output-dir>");
            System.exit(1);
        }

        final int pid = Integer.parseInt(args[0]);
        final int nprocs = Integer.parseInt(args[1]);
        final int minHeight = Integer.parseInt(args[2]);
        final int minWidth = Integer.parseInt(args[3]);
        final String inputDir = args[4];
        final String outputDir = args[5];

        final File inputDirFile = new File(inputDir);
        String[] inputFilenames = inputDirFile.list();

        long startTime = System.currentTimeMillis();
       
        int nprocessed = 0;
        for (int i = 0; i < inputFilenames.length; i++) {
            if (i % nprocs != pid) {
                continue;
            }

            final String inputPath = inputDir + "/" + inputFilenames[i];
            assert(inputPath.endsWith(".JPEG"));
            final String outputPath = outputDir + "/" + inputFilenames[i] + ".txt";

            if (new File(outputPath).exists()) {
                continue;
            }

            final BufferedImage img;
            try {
                img = ImageIO.read(new File(inputPath));
            } catch (Exception e) {
                System.err.println("Error on file " + inputPath);
                throw new RuntimeException(e);
            }

            final BufferedWriter writer;
            try {
                writer = new BufferedWriter(new FileWriter(new File(outputPath)));
            } catch (IOException io) {
                throw new RuntimeException(io);
            }

            final Raster raster = img.getData();
            // final int width = raster.getWidth();
            // final int height = raster.getHeight();
            final int height = minHeight;
            final int width = minWidth;
            double[] sample = new double[3];

            final StringBuilder sb = new StringBuilder();
            for (int r = 0; r < height; r++) {
                for (int c = 0; c < width; c++) {
                    sample = raster.getPixel(c, r, sample);
                    for (int s = 0; s < sample.length; s++) {
                        sb.append((int)sample[s]);
                        sb.append(" ");
                    }
                }
            }

            try {
                writer.write(sb.toString());
                writer.close();
            } catch (IOException io) {
                throw new RuntimeException(io);
            }

            nprocessed++;
            final double msPerImage =
                (double)(System.currentTimeMillis() - startTime) /
                (double)(nprocessed);
            System.out.println(nprocessed + " " + msPerImage + " " + inputPath);
        }
    }
}
