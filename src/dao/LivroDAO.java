package dao;

import database.Database;
import model.Livro;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LivroDAO {


    public void salvar(Livro livro) {
        String sql = "INSERT INTO livros (titulo, autor, ano, disponivel) VALUES (?, ?, ?, ?)";
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, livro.getTitulo());
            stmt.setString(2, livro.getAutor());
            stmt.setInt(3, livro.getAno());
            stmt.setBoolean(4, livro.isDisponivel());
            stmt.executeUpdate();

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) livro.setId(rs.getInt(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao salvar livro: " + e.getMessage(), e);
        }
    }

    public List<Livro> listar() {
        List<Livro> livros = new ArrayList<>();
        String sql = "SELECT id, titulo, autor, ano, disponivel FROM livros ORDER BY id";
        try (Connection conn = Database.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                livros.add(new Livro(
                        rs.getInt("id"),
                        rs.getString("titulo"),
                        rs.getString("autor"),
                        rs.getInt("ano"),
                        rs.getBoolean("disponivel")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao listar livros: " + e.getMessage(), e);
        }
        return livros;
    }

   
    public Livro buscarPorId(int id) {
        String sql = "SELECT id, titulo, autor, ano, disponivel FROM livros WHERE id = ?";
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new Livro(
                            rs.getInt("id"),
                            rs.getString("titulo"),
                            rs.getString("autor"),
                            rs.getInt("ano"),
                            rs.getBoolean("disponivel")
                    );
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao buscar livro: " + e.getMessage(), e);
        }
    }

    
    public void atualizar(Livro livro) {
        String sql = "UPDATE livros SET titulo = ?, autor = ?, ano = ?, disponivel = ? WHERE id = ?";
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, livro.getTitulo());
            stmt.setString(2, livro.getAutor());
            stmt.setInt(3, livro.getAno());
            stmt.setBoolean(4, livro.isDisponivel());
            stmt.setInt(5, livro.getId());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao atualizar livro: " + e.getMessage(), e);
        }
    }

    
    public void deletar(int id) {
        String sql = "DELETE FROM livros WHERE id = ?";
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao deletar livro: " + e.getMessage(), e);
        }
    }

    public void atualizarDisponibilidade(int livroId, boolean disponivel) {
        String sql = "UPDATE livros SET disponivel = ? WHERE id = ?";
        try (Connection conn = Database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setBoolean(1, disponivel);
            stmt.setInt(2, livroId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao atualizar disponibilidade do livro: " + e.getMessage(), e);
        }
    }

    // Atalhos práticos
    public void marcarIndisponivel(int livroId) { atualizarDisponibilidade(livroId, false); }
    public void marcarDisponivel(int livroId)   { atualizarDisponibilidade(livroId, true);  }
}
