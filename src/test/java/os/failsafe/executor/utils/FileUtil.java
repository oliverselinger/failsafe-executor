package os.failsafe.executor.utils;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {

    public static String readResourceFile(String name) {
        try {
            URI uri = FileUtil.class.getClassLoader().getResource(name).toURI();
            return new String(Files.readAllBytes(Paths.get(uri)), Charset.defaultCharset());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
