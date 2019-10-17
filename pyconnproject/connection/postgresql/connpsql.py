import psycopg2

# Abre a conexão com o banco de dados
try:
    conn = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres",
    dbname="pydb",
    port="5432")

    # abrindo o cursos - responsável pela instância de persistência (realiza as transações a partir da conexão do banco de
    # dados corrente)
    cursor = conn.cursor()

    # Cria o schema empresaschema
    cursor.execute("drop schema if exists empresaschema cascade;")
    cursor.execute("create schema if not exists empresaschema;")

    #Cria a tabela funcionário
    cursor.execute("CREATE TABLE empresaschema.funcionario ("
                   "pnome text NOT NULL, "
                   "minicial text, unome text NOT NULL, "
                   "cpf integer PRIMARY KEY, "
                   "datanasc date, "
                   "endereco text, "
                   "sexo text, "
                   "salario float DEFAULT 0, "
                   "cpf_supervisor int, "
                   "dnr int);")

    #Cria a tabeça departamento
    cursor.execute("CREATE TABLE empresaschema.departamento ("
                   "dnome text NOT NULL, "
                   "dnumero int PRIMARY KEY , "
                   "cpf_gerente int DEFAULT 0, "
                   "data_inicio_gerente date);")

    #Adiciona integridade referencial
    cursor.execute("ALTER TABLE ONLY empresaschema.funcionario "
                   "ADD CONSTRAINT numerodepto FOREIGN KEY (dnr) "
                   "REFERENCES empresaschema.departamento(dnumero);")

    #insere dados
    cursor.execute("INSERT INTO empresaschema.departamento (dnome, dnumero, cpf_gerente, data_inicio_gerente) VALUES "
                   "('Sede_administrativa',1, 888665555,'1981-06-19'),"
                   "('Computacao',5, 333445555, '1990-06-30');")

    cursor.execute("INSERT INTO empresaschema.funcionario (pnome, minicial, unome, cpf, datanasc, endereco, sexo, salario, cpf_supervisor, dnr) VALUES "
                   "('James', 'E', 'Borg', 888665555, '1937-11-10', '450 Stone, Houston, TX', 'M' , 55000, 12345678, 1), "
                   "('Robert', 'F', 'Scott', 943775543, '2042-06-21', '2365 Newcastle Rd, Bellaire, TX', 'M' , 58000, 888665555, 1), "
                   "('Franklin', 'T', 'Wong', 333445555, '1955-12-08', '638 Voss, Housto, TX', 'M' , 40000, 888665555, 5), "
                   "('John', 'B', 'Smith', 123456789, '1965-01-09', '731 Fondren, Houston, TX', 'M' , 30000, 333445555, 5);")


    cursor.execute("select * from empresaschema.funcionario;")
    # Recupera todos os funcionários
    print("Exemplo: FETCHALL")
    for row in cursor.fetchall():
        print(row)

    cursor.execute("select * from empresaschema.funcionario;")
    # Recupera o primeiro funcionário
    row = cursor.fetchone();
    print("Exemplo: FETCHONE")
    print(row)

    cursor.execute("select * from empresaschema.funcionario;")
    # Recupera os três primeiros funcionários
    print("Exemplo: FETCHMANY")
    for row in cursor.fetchmany(3):
        print(row)

    # desconecta o cursor
    cursor.close()
    conn.commit()


except psycopg2.Error as error:
    # desconecta do servidor
    print("Erro de conexão com o postgresql " + error)

finally:
    if (conn):
        cursor.close()
        conn.close()
        print("postgresql finalizado")