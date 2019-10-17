import pymysql

try:
    # Abre a conexão com o banco de dados
    conn = pymysql.connect(host="localhost",
                           user="root",
                           password="root",
                           db="pydb",
                           port=3306)

    # Abre o cursos
    cursor = conn.cursor()

    # Executa o comando SQL
    cursor.execute("select version()")

    # Retorna uma tupla
    data = cursor.fetchone()
    print("Database version: %s " % data)

    # Desconecta o cursor
    cursor.close()

except pymysql.Error as error:
    print("Erro de conexão com o mysql " + error)

finally:
    # Desconecta do servidor
    conn.close()


