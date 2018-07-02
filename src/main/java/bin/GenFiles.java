package bin;

import config.Config;
import exception.GeneratorException;
import filegen.FileGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import writer.MultiFileWriter;

import java.io.IOException;

public class GenFiles {

    private static final Logger logger = LoggerFactory.getLogger(GenFiles.class);

    private FileGenerator generator;
    private MultiFileWriter fileWriter;

    public GenFiles(String[] levelPrefixes, int[] levelInstanceNums,
                    int fileSizeInByte, String outputFilePath, int maxEntryPerFile) throws GeneratorException, IOException {
        this.generator = new FileGenerator(levelPrefixes, levelInstanceNums, fileSizeInByte);
        this.fileWriter = new MultiFileWriter(outputFilePath, maxEntryPerFile);
    }

    private void generate() throws IOException {
        while (generator.hasNext()) {
            generator.next();
            fileWriter.write(generator.getCurrPath(), generator.getCurrContent());
        }
        fileWriter.close();
    }

    public static void help() {
        System.out.println("Usages:\n" +
                "    Using command line args:\n" +
                "        Args : <levelPrefixes(comma separated)> <levelInstanceNumbers(comma separated)> <fileSizeInByte> <outputFilePath> <maxEntryPerFile>\n" +
                "        Example : P,M,L,T,I 10,80,50,6,80 819200 output/compact 1000000\n" +
                "    Using config file:\n" +
                "        Args : -conf <config file path>\n" +
                "        Example : -conf config/config\n" +
                "    Using default config:\n" +
                "        No args");
    }

    /*
    Usages:
    Using command line args:
        Args : <levelPrefixes(comma separated)> <levelInstanceNumbers(comma separated)> <fileSizeInByte> <outputFilePath> <maxEntryPerFile>
        Example : P,M,L,T,I 10,80,50,6,80 819200 output/compact 1000000
    Using config file:
        Args : -conf <config file path>
        Example : -conf config/config
    Using default config:
        No args
     */
    public static void main(String[] args) {
        String[] levelPrefixes = null;
        int maxEntryPerFile = 0;
        int fileSizeInByte = 0;
        String outputFilePath = null;
        int[] instanceNumnber = null;

        if (args.length == 5) {
            levelPrefixes = args[0].split(".");
            String[] strInstanceNumbers = args[1].split(".");
            instanceNumnber = new int[strInstanceNumbers.length];
            for (int i = 0; i < instanceNumnber.length; i++) {
                instanceNumnber[i] = Integer.parseInt(strInstanceNumbers[i]);
            }
            fileSizeInByte = Integer.parseInt(args[2]);
            outputFilePath = args[3];
            maxEntryPerFile = Integer.parseInt(args[4]);
        } else if (args.length == 2 && "-conf".equals(args[0])) {
            Config.loadConfig(args[1]);
            levelPrefixes = Config.LEVER_PREFIXES;
            instanceNumnber = Config.INSTANCE_NUMBERS;
            maxEntryPerFile = Config.MAX_ENTRY_PER_FILE;
            fileSizeInByte = Config.FILE_SIZE_IN_BYTE;
            outputFilePath = Config.COMPACT_OUTPUT;
        } else if (args.length == 0) {
            levelPrefixes = Config.LEVER_PREFIXES;
            instanceNumnber = Config.INSTANCE_NUMBERS;
            maxEntryPerFile = Config.MAX_ENTRY_PER_FILE;
            fileSizeInByte = Config.FILE_SIZE_IN_BYTE;
            outputFilePath = Config.COMPACT_OUTPUT;
        } else {
            help();
            return;
        }

        GenFiles genFiles;
        try {
            genFiles = new GenFiles(levelPrefixes, instanceNumnber, fileSizeInByte, outputFilePath, maxEntryPerFile);
        } catch (GeneratorException | IOException e) {
            logger.error("Error encountered when initialize generator : ", e);
            return;
        }
        try {
            genFiles.generate();
        } catch (IOException e) {
            logger.error("Error encountered when generating : ", e);
        }
    }
}
