package dao;
//Importação das bibliotecas
import database.Database;
import model.Emprestimo;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
/* 
 * DAO de empretimos 
 * Regras prioncipais que este DAO garante:
 * 1) Um livro NÃO pode ser emprestado se já estiver emprestado (disponivel = 0).
 * 2) Sempre que um emprestimo é criado : o livro é marcado como indiponivel (disponivel = 0).
 *
 *  Como garantimos consistencia??
 * 
 * Observações:
 * 
 */


public class EmprestimoDAO {

    // ---------- CREATE (INSERIR)  ----------
    public void salvar(Emprestimo e) {
        String sqlSelectLivro = "SELECT disponivel FROM livros WHERE id = ? FOR UPDATE";//Consulta a disponibilidade
        String sqlInsertEmp   = "INSERT INTO emprestimos (id_livro, id_usuario, data_emprestimo, data_devolucao) VALUES (?, ?, ?, ?)";// Insere imprestimo
        String sqlBlockLivro  = "UPDATE livros SET disponivel = 0 WHERE id = ?"; // Marca o livro como indiponivel

        try (Connection conn = Database.getConnection()) {
            conn.setAutoCommit(false);

            // 1) Trava e checa disponibilidade do livro
            try (PreparedStatement ps = conn.prepareStatement(sqlSelectLivro)) {
                ps.setInt(1, e.getLivroId());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        throw new RuntimeException("Livro não encontrado.");
                    }
                    boolean disponivel = rs.getBoolean(1);
                    if (!disponivel) {
                        conn.rollback();
                        throw new RuntimeException("Este livro já está emprestado no momento.");
                    }
                }
            }

            // 2) Insere empréstimo
            try (PreparedStatement ps = conn.prepareStatement(sqlInsertEmp, Statement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, e.getLivroId());
                ps.setInt(2, e.getUsuarioId());
                ps.setDate(3, Date.valueOf(e.getDataEmprestimo()));
                ps.setDate(4, Date.valueOf(e.getDataDevolucao()));
                ps.executeUpdate();

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) e.setId(rs.getInt(1));
                }
            }

            // 3) Marca livro como indisponível
            try (PreparedStatement ps = conn.prepareStatement(sqlBlockLivro)) {
                ps.setInt(1, e.getLivroId());
                ps.executeUpdate();
            }

            conn.commit();
        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao salvar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // ---------- READ ----------
    public List<Emprestimo> listar() {
        List<Emprestimo> lista = new ArrayList<>();
        String sql = "SELECT id, id_livro, id_usuario, data_emprestimo, data_devolucao FROM emprestimos ORDER BY id DESC";
        try (Connection conn = Database.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                lista.add(map(rs));
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao listar empréstimos: " + ex.getMessage(), ex);
        }
        return lista;
    }

    public Emprestimo buscarPorId(int id) {
        String sql = "SELECT id, id_livro, id_usuario, data_emprestimo, data_devolucao FROM emprestimos WHERE id = ?";
        try (Connection conn = Database.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return map(rs);
                return null;
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao buscar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // ---------- UPDATE (com transação + troca de livro segura) ----------
    public void atualizar(Emprestimo e) {
        Emprestimo antes = buscarPorId(e.getId());
        if (antes == null) throw new RuntimeException("Empréstimo não encontrado.");

        String sqlSelectLivro = "SELECT disponivel FROM livros WHERE id = ? FOR UPDATE";
        String sqlUpdateEmp   = "UPDATE emprestimos SET id_livro = ?, id_usuario = ?, data_emprestimo = ?, data_devolucao = ? WHERE id = ?";
        String sqlBlockLivro  = "UPDATE livros SET disponivel = 0 WHERE id = ?";
        String sqlFreeLivro   = "UPDATE livros SET disponivel = 1 WHERE id = ?";

        try (Connection conn = Database.getConnection()) {
            conn.setAutoCommit(false);

            // 1) Se o livro mudou, verificar novo livro e travar
            if (antes.getLivroId() != e.getLivroId()) {
                try (PreparedStatement ps = conn.prepareStatement(sqlSelectLivro)) {
                    ps.setInt(1, e.getLivroId());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            throw new RuntimeException("Novo livro não encontrado.");
                        }
                        boolean disponivel = rs.getBoolean(1);
                        if (!disponivel) {
                            conn.rollback();
                            throw new RuntimeException("O novo livro já está emprestado.");
                        }
                    }
                }
            }

            // 2) Atualiza o empréstimo
            try (PreparedStatement ps = conn.prepareStatement(sqlUpdateEmp)) {
                ps.setInt(1, e.getLivroId());
                ps.setInt(2, e.getUsuarioId());
                ps.setDate(3, Date.valueOf(e.getDataEmprestimo()));
                ps.setDate(4, Date.valueOf(e.getDataDevolucao()));
                ps.setInt(5, e.getId());
                ps.executeUpdate();
            }

            // 3) Ajusta disponibilidade dos livros se trocou
            if (antes.getLivroId() != e.getLivroId()) {
                // libera antigo
                try (PreparedStatement ps = conn.prepareStatement(sqlFreeLivro)) {
                    ps.setInt(1, antes.getLivroId());
                    ps.executeUpdate();
                }
                // bloqueia novo
                try (PreparedStatement ps = conn.prepareStatement(sqlBlockLivro)) {
                    ps.setInt(1, e.getLivroId());
                    ps.executeUpdate();
                }
            }

            conn.commit();
        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao atualizar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // ---------- DELETE (com transação + liberar livro) ----------
    public void deletar(int id) {
        Emprestimo emp = buscarPorId(id);
        if (emp == null) return;

        String sqlDeleteEmp = "DELETE FROM emprestimos WHERE id = ?";
        String sqlFreeLivro  = "UPDATE livros SET disponivel = 1 WHERE id = ?";

        try (Connection conn = Database.getConnection()) {
            conn.setAutoCommit(false);

            // 1) Exclui o empréstimo
            try (PreparedStatement ps = conn.prepareStatement(sqlDeleteEmp)) {
                ps.setInt(1, id);
                ps.executeUpdate();
            }

            // 2) Libera o livro
            try (PreparedStatement ps = conn.prepareStatement(sqlFreeLivro)) {
                ps.setInt(1, emp.getLivroId());
                ps.executeUpdate();
            }

            conn.commit();
        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao deletar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // ---------- Helper ----------
    private Emprestimo map(ResultSet rs) throws SQLException {
        int id          = rs.getInt("id");
        int idLivro     = rs.getInt("id_livro");
        int idUsuario   = rs.getInt("id_usuario");
        LocalDate dtEmp = rs.getDate("data_emprestimo").toLocalDate();
        LocalDate dtDev = rs.getDate("data_devolucao").toLocalDate();
        return new Emprestimo(id, idLivro, idUsuario, dtEmp, dtDev);
    }
}
