package br.com.juniorrodrigues;

public class User {

    private final String uuid;

    public User(String uuid) {
        this.uuid = uuid;
    }

    public String getReportPath() {
        return "target/" + uuid + "-report.txt";// novo arquivo de relatorio
    }

    public String getUuid() {
        return uuid;
    }
}
