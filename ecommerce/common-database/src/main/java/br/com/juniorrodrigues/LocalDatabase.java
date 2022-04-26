package br.com.juniorrodrigues;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;
    // Assumindo que esse serviço via dor uma unica vez, criarei uma conexão com o um banco de dados pra salvar aas informçeos
    public LocalDatabase(String name) throws SQLException {
        // aqui no construtor vamos aabrir a conexao com o banco
        String url ="jdbc:sqlite:target/"+ name +".db";// criando o arquivo dentro do diretotio target do projeto
        this.connection = DriverManager.getConnection(url);
    }
    // yes this is way to generic.
    // according to your database tool, avoid injection.
    public void createIfNotExists(String sql){
        try {
            connection.createStatement().execute(sql);
        }catch (SQLException ex){
            // be careful, the sql could be wrong, be really careful
            ex.printStackTrace();
        }
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var prepareStatement = connection.prepareStatement(statement); //o statement pra manipular o banco, no caso um insert
        for (int i = 0; i < params.length; i++) {
            prepareStatement.setString(i + 1, params[i]);
        }
        return prepareStatement;
    }

    public boolean update(String statement, String ... params) throws SQLException {
        return prepare(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }

    public void close() throws SQLException {
        connection.close();
    }
}
