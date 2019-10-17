from connection.postgresql import ConexaoPSQL

try:
    # Abre a conexão com o banco de dados
    db = ConexaoPSQL.ConexaoPSQL("localhost", "pydb", "postgres", "postgres", "5432")

    #sql para criar o schema empresaschema
    sqldropchema = "drop schema if exists empresaschema cascade;"
    sqlcreateschema = "create schema if not exists empresaschema;"

    #sql para criar a tabela funcionário
    sqlfunc = "CREATE TABLE empresaschema.funcionario (pnome text NOT NULL, minicial text, unome text NOT NULL, cpf integer PRIMARY KEY, datanasc date, endereco text, sexo text, salario float DEFAULT 0, cpf_supervisor int, dnr int);"

    #sql para cria a tabeça departamento
    sqldep = "CREATE TABLE empresaschema.departamento (dnome text NOT NULL, dnumero int PRIMARY KEY , cpf_gerente int DEFAULT 0, data_inicio_gerente date);"

    #sql para adiciona a integridade referencial entre funcionário e departamento
    sqlconstraint = "ALTER TABLE ONLY empresaschema.funcionario ADD CONSTRAINT numerodepto FOREIGN KEY (dnr) REFERENCES empresaschema.departamento(dnumero);"
    
    #sql para inserir dados
    sqlinsertdep = "INSERT INTO empresaschema.departamento (dnome, dnumero, cpf_gerente, data_inicio_gerente) VALUES ('Sede_administrativa',1, 888665555,'1981-06-19'), ('Computacao',5, 333445555, '1990-06-30');"
    sqlinsertfunc = "INSERT INTO empresaschema.funcionario (pnome, minicial, unome, cpf, datanasc, endereco, sexo, salario, cpf_supervisor, dnr) VALUES ('James', 'E', 'Borg', 888665555, '1937-11-10', '450 Stone, Houston, TX', 'M' , 55000, 12345678, 1), ('Robert', 'F', 'Scott', 943775543, '2042-06-21', '2365 Newcastle Rd, Bellaire, TX', 'M' , 58000, 888665555, 1), ('Franklin', 'T', 'Wong', 333445555, '1955-12-08', '638 Voss, Housto, TX', 'M' , 40000, 888665555, 5), ('John', 'B', 'Smith', 123456789, '1965-01-09', '731 Fondren, Houston, TX', 'M' , 30000, 333445555, 5);"

    # chamada do método de manipulação para os sqls definidos acima
    db.manipular(sqldropchema)
    db.manipular(sqlcreateschema)
    db.manipular(sqlfunc)
    db.manipular(sqldep)
    db.manipular(sqlconstraint)
    db.manipular(sqlinsertdep)
    db.manipular(sqlinsertfunc)

    sqlselectfunc = "select * from empresaschema.funcionario;"
    # Recupera todos os funcionários
    for row in db.consultar(sqlselectfunc):
        print(row)

except ConexaoPSQL.Error as error:
    print("Erro de conexão " + error)

finally:
    if (db):
        db.fechar()
        print("FIM")