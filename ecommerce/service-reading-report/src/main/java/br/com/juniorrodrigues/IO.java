package br.com.juniorrodrigues;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public class IO {
    public static void copyTo(Path source, File target) throws IOException {
        target.getParentFile().mkdirs(); //garante que o reporitori oexista, pegando parete e criar ops diretorios necessarios
        Files.copy(source, target.toPath(), StandardCopyOption.REPLACE_EXISTING); //copia do source pro , se ja existe substitui o arquivo
    }

    public static void append(File target, String content) throws IOException {
        Files.write(target.toPath(), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }

}
