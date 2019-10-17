import psycopg2

class ConexaoPSQL(object):

    _db = None

    # Inicialização com passagem de parâmetro
    def __init__(self, mhost, db, pwd, usr, port):
        try:
            self._db = psycopg2.connect(host=mhost, database=db, password=pwd, user=usr, port=port)
        except psycopg2.Error as error:
            print("Erro ao se conectar no banco de dados!" + error)


    def manipular(self, sql):
        try:
            cur = self._db.cursor()
            cur.execute(sql)
            cur.close()
            self._db.commit()
        except psycopg2.Error as error:
            print(error)
            return False
        return True

    def consultar(self, sql):
        rs = None
        try:
            cur = self._db.cursor()
            cur.execute(sql)
            rs = cur.fetchall()
        except psycopg2.Error as error:
            print(error)
            return None
        return rs

    def proximaPK(self, tabela, chave):
        sql = 'select max(' + chave + ') from ' + tabela
        rs = self.consultar(sql)
        pk = rs[0][0]
        return pk + 1


    def fechar(self):
        self._db.close()
