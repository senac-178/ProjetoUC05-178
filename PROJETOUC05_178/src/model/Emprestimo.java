package model;

import java.time.LocalDate;

public class Emprestimo {
    private int id;
    private int livroId;
    private int usuarioId;
    private LocalDate dataEmprestimo;
    private LocalDate dataDevolucao;

    public Emprestimo(int id, int livroId, int usuarioId, LocalDate dataEmprestimo, LocalDate dataDevolucao) { 
        //Contrutor
        this.id = id;
        this.livroId = livroId;
        this.usuarioId = usuarioId;
        this.dataEmprestimo = dataEmprestimo;
        this.dataDevolucao = dataDevolucao;
    }

    // Getters e Setters -  Modificadores de acesso
    public int getId() { 
        return id; 
    }

    public void setId(int id) { 
        this.id = id; 
    }

    public int getLivroId() { 
        return livroId; 
    }

    public void setLivroId(int livroId) { 
        this.livroId = livroId; 
    }

    public int getUsuarioId() { 
        return usuarioId; 
    }

    public void setUsuarioId(int usuarioId) { 
        this.usuarioId = usuarioId; 
    }

    public LocalDate getDataEmprestimo() { 
        return dataEmprestimo; 
    }

    public void setDataEmprestimo(LocalDate dataEmprestimo) { 
        this.dataEmprestimo = dataEmprestimo; 
    }

    public LocalDate getDataDevolucao() { 
        return dataDevolucao; 
    }

    public void setDataDevolucao(LocalDate dataDevolucao) { 
        this.dataDevolucao = dataDevolucao; 
    }
}
