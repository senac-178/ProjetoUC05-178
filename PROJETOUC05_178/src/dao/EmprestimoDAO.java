package dao;

import database.Database;
import model.Emprestimo;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * DAO de Empréstimos
 *
 * Regras principais que este DAO garante:
 * 1) Um livro NÃO pode ser emprestado se já estiver emprestado (disponivel = 0).
 * 2) Sempre que um empréstimo é criado: o livro é marcado como indisponível (disponivel = 0).
 * 3) Sempre que um empréstimo é excluído: o livro volta a ficar disponível (disponivel = 1).
 * 4) Ao atualizar um empréstimo trocando o livro, o livro antigo é liberado e o novo é bloqueado.
 *
 * Como garantimos consistência?
 * - Usamos TRANSAÇÕES (setAutoCommit(false) / commit / rollback)
 * - Usamos "SELECT ... FOR UPDATE" para travar a linha do livro enquanto checamos disponibilidade
 *   (evita duas pessoas pegarem o mesmo livro ao mesmo tempo).
 *
 * Observação importante: para "FOR UPDATE" funcionar você precisa que sua tabela esteja em
 * mecanismo transacional (ex.: InnoDB) e a conexão não esteja em autocommit.
 */
public class EmprestimoDAO {

    // =====================================================================
    // CREATE (INSERIR) - com transação e lock pessimista no livro
    // =====================================================================
    public void salvar(Emprestimo e) {
        // 1) Consulta a disponibilidade travando a linha do livro (FOR UPDATE)
        final String sqlSelectLivro =
                "SELECT disponivel FROM livros WHERE id = ? FOR UPDATE";

        // 2) Insere o empréstimo
        final String sqlInsertEmp =
                "INSERT INTO emprestimos (id_livro, id_usuario, data_emprestimo, data_devolucao) " +
                "VALUES (?, ?, ?, ?)";

        // 3) Marca o livro como indisponível
        final String sqlBlockLivro =
                "UPDATE livros SET disponivel = 0 WHERE id = ?";

        try (Connection conn = Database.getConnection()) {
            // Inicia transação (desliga autocommit)
            conn.setAutoCommit(false);

            // --- (1) Trava e checa disponibilidade do livro ---
            try (PreparedStatement ps = conn.prepareStatement(sqlSelectLivro)) {
                ps.setInt(1, e.getLivroId());
                try (ResultSet rs = ps.executeQuery()) {

                    // Se não achou o livro, algo está errado
                    if (!rs.next()) {
                        conn.rollback();
                        throw new RuntimeException("Livro não encontrado.");
                    }

                    // Se o livro NÃO está disponível, aborta operação
                    boolean disponivel = rs.getBoolean(1);
                    if (!disponivel) {
                        conn.rollback();
                        throw new RuntimeException("Este livro já está emprestado no momento.");
                    }
                }
            }

            // --- (2) Insere o empréstimo ---
            try (PreparedStatement ps = conn.prepareStatement(sqlInsertEmp, Statement.RETURN_GENERATED_KEYS)) {
                ps.setInt(1, e.getLivroId());
                ps.setInt(2, e.getUsuarioId());
                ps.setDate(3, Date.valueOf(e.getDataEmprestimo())); // LocalDate -> java.sql.Date
                ps.setDate(4, Date.valueOf(e.getDataDevolucao()));
                ps.executeUpdate();

                // Recupera o ID gerado (auto_increment) e atribui ao objeto
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        e.setId(rs.getInt(1));
                    }
                }
            }

            // --- (3) Marca o livro como indisponível ---
            try (PreparedStatement ps = conn.prepareStatement(sqlBlockLivro)) {
                ps.setInt(1, e.getLivroId());
                ps.executeUpdate();
            }

            // Se chegou aqui, tudo OK — confirma a transação
            conn.commit();

        } catch (SQLException ex) {
            // Em caso de erro, encapsula a SQLException em RuntimeException
            // (você pode tratar de outra forma se preferir)
            throw new RuntimeException("Erro ao salvar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // =====================================================================
    // READ (LISTAR TODOS)
    // =====================================================================
    public List<Emprestimo> listar() {
        List<Emprestimo> lista = new ArrayList<>();

        final String sql =
                "SELECT id, id_livro, id_usuario, data_emprestimo, data_devolucao " +
                "FROM emprestimos " +
                "ORDER BY id DESC";

        try (Connection conn = Database.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            // Para cada linha, transformamos em um objeto Emprestimo (via helper map)
            while (rs.next()) {
                lista.add(map(rs));
            }

        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao listar empréstimos: " + ex.getMessage(), ex);
        }
        return lista;
    }

    // =====================================================================
    // READ (BUSCAR POR ID)
    // =====================================================================
    public Emprestimo buscarPorId(int id) {
        final String sql =
                "SELECT id, id_livro, id_usuario, data_emprestimo, data_devolucao " +
                "FROM emprestimos " +
                "WHERE id = ?";

        try (Connection conn = Database.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, id);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return map(rs); // converte a linha do ResultSet em um Emprestimo
                }
                return null;
            }

        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao buscar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // =====================================================================
    // UPDATE (ATUALIZAR) - com transação / libera antigo / bloqueia novo
    // =====================================================================
    public void atualizar(Emprestimo e) {
        // Busca o estado anterior do empréstimo.
        // Precisamos disso para saber se o livro foi trocado.
        Emprestimo antes = buscarPorId(e.getId());
        if (antes == null) {
            throw new RuntimeException("Empréstimo não encontrado.");
        }

        // Se o livro for trocado, checamos o novo livro (com FOR UPDATE)
        final String sqlSelectLivro =
                "SELECT disponivel FROM livros WHERE id = ? FOR UPDATE";

        // Atualiza os campos do empréstimo
        final String sqlUpdateEmp =
                "UPDATE emprestimos SET id_livro = ?, id_usuario = ?, data_emprestimo = ?, data_devolucao = ? " +
                "WHERE id = ?";

        // Marca livro como indisponível
        final String sqlBlockLivro =
                "UPDATE livros SET disponivel = 0 WHERE id = ?";

        // Marca livro como disponível
        final String sqlFreeLivro  =
                "UPDATE livros SET disponivel = 1 WHERE id = ?";

        try (Connection conn = Database.getConnection()) {
            conn.setAutoCommit(false);

            // --- (1) Se trocou de livro, verifica o novo e trava a linha ---
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

            // --- (2) Atualiza o empréstimo ---
            try (PreparedStatement ps = conn.prepareStatement(sqlUpdateEmp)) {
                ps.setInt(1, e.getLivroId());
                ps.setInt(2, e.getUsuarioId());
                ps.setDate(3, Date.valueOf(e.getDataEmprestimo()));
                ps.setDate(4, Date.valueOf(e.getDataDevolucao()));
                ps.setInt(5, e.getId());
                ps.executeUpdate();
            }

            // --- (3) Se trocou de livro: libera o antigo e bloqueia o novo ---
            if (antes.getLivroId() != e.getLivroId()) {
                // Libera o livro antigo (volta disponivel = 1)
                try (PreparedStatement ps = conn.prepareStatement(sqlFreeLivro)) {
                    ps.setInt(1, antes.getLivroId());
                    ps.executeUpdate();
                }

                // Bloqueia o novo livro (disponivel = 0)
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

    // =====================================================================
    // DELETE (DELETAR) - com transação / libera o livro
    // =====================================================================
    public void deletar(int id) {
        // Primeiro, buscamos o empréstimo para saber qual livro liberar depois
        Emprestimo emp = buscarPorId(id);
        if (emp == null) {
            return; // nada a fazer
        }

        // Exclui o registro do empréstimo
        final String sqlDeleteEmp =
                "DELETE FROM emprestimos WHERE id = ?";

        // Libera o livro (volta para disponivel = 1)
        final String sqlFreeLivro  =
                "UPDATE livros SET disponivel = 1 WHERE id = ?";

        try (Connection conn = Database.getConnection()) {
            conn.setAutoCommit(false);

            // --- (1) Exclui o empréstimo ---
            try (PreparedStatement ps = conn.prepareStatement(sqlDeleteEmp)) {
                ps.setInt(1, id);
                ps.executeUpdate();
            }

            // --- (2) Libera o livro associado ---
            try (PreparedStatement ps = conn.prepareStatement(sqlFreeLivro)) {
                ps.setInt(1, emp.getLivroId());
                ps.executeUpdate();
            }

            conn.commit();

        } catch (SQLException ex) {
            throw new RuntimeException("Erro ao deletar empréstimo: " + ex.getMessage(), ex);
        }
    }

    // =====================================================================
    // Helper: converte a linha do ResultSet em um objeto Emprestimo
    // =====================================================================
    private Emprestimo map(ResultSet rs) throws SQLException {
        // Lê as colunas pelo NOME (iguais aos nomes do banco)
        int id            = rs.getInt("id");
        int idLivro       = rs.getInt("id_livro");
        int idUsuario     = rs.getInt("id_usuario");

        // Datas no banco são DATE -> no Java usamos java.time.LocalDate
        LocalDate dtEmp   = rs.getDate("data_emprestimo").toLocalDate();
        LocalDate dtDev   = rs.getDate("data_devolucao").toLocalDate();

        // Retorna um novo objeto Emprestimo preenchido
        return new Emprestimo(id, idLivro, idUsuario, dtEmp, dtDev);
    }
}
