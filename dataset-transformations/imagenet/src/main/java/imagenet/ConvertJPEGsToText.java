import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.awt.image.Raster;

public class ConvertJPEGsToText {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("usage: ConvertJPEGsToText <process-id> <nprocesses> <input-dir> <output-dir>");
            System.exit(1);
        }

        final int pid = Integer.parseInt(args[0]);
        final int nprocs = Integer.parseInt(args[1]);
        final String inputDir = args[2];
        final String outputDir = args[3];

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

            final BufferedImage img;
            try {
                img = ImageIO.read(new File(inputPath));
            } catch (IOException io) {
                throw new RuntimeException(io);
            }

            final BufferedWriter writer;
            try {
                writer = new BufferedWriter(new FileWriter(new File(outputPath)));
            } catch (IOException io) {
                throw new RuntimeException(io);
            }

            final Raster raster = img.getData();
            final int width = raster.getWidth();
            final int height = raster.getHeight();
            double[] sample = new double[3];

            // System.err.println(height + " " + width + " " + raster.getMinX() + " " + raster.getMinY());

            final StringBuilder sb = new StringBuilder();
            for (int r = 0; r < height; r++) {
                for (int c = 0; c < width; c++) {
                    sample = raster.getPixel(c, r, sample);
                    for (int s = 0; s < sample.length; s++) {
                        sb.append(sample[s]);
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
