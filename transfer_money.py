import sys
import pymysql

class TransferMoney(object):
    def __init__(self, conn):
        self.conn = conn

    def check_acct_availlable(self, acctid):
        cursor = self.conn.cursor()
        try: 
            sql = "select * from account where acctid=%s" % acctid
            cursor.execute(sql)
            print("check_acct_availlable:" + sql)
            rs = cursor.fetchall()
            if len(rs) != 1:
                raise Exception("帐号%s不存在" % acctid)
        finally:
            cursor.close()

            

    def has_enough_money(self, acctid, money):
        cursor = self.conn.cursor()
        try: 
            sql = "select * from account where acctid=%s and money>%s" % (acctid, money)            
            cursor.execute(sql)
            print("has_enough_money:" + sql)
            rs = cursor.fetchall()
            if len(rs) != 1:
                raise Exception("帐号%s没有足够的钱" % acctid)
        finally:
            cursor.close()
        
    def reduce_money(self,acctid,money):
        cursor = self.conn.cursor()
        try: 
            sql = "update account set money=money-%s where acctid=%s" % (money, acctid)            
            cursor.execute(sql)
            print("reduce_money:" + sql)
            rs = cursor.fetchall()
            # 表示查看这条sql语句影响了表中多少行数据，若不为1，则执行失败
            if cursor.rowcount != 1:
                raise Exception("帐号%s减歀失败" % acctid)
        finally:
            cursor.close()
        

    def add_money(self, acctid, money):
        cursor = self.conn.cursor()
        try: 
            sql = "update account set money=money+%s where acctid=%s" % (money, acctid)            
            cursor.execute(sql)
            print("add_money" + sql)
            rs = cursor.fetchall()
            if cursor.rowcount != 1:
                raise Exception("帐号%s加款失败" % acctid)
        finally:
            cursor.close()
        

    def transfer(self, source_acctid, target_acctid, money):
        try:
            # 检查帐号是否可用
            self.check_acct_availlable(source_acctid)
            self.check_acct_availlable(target_acctid)
            self.has_enough_money(source_acctid, money)
            self.reduce_money(source_acctid, money)
            self.add_money(target_acctid, money)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

if __name__ == "__main__":
    source_acctid = 11
    target_acctid = 12
    money = 100

    conn = pymysql.Connect(host = 'localhost',
                       port = 3306,
                       user = 'root', 
                       passwd = 'zhy199533',
                       db = 'imooc', 
                       charset = 'utf8') 
    tr_money = TransferMoney(conn) 

    try:
        tr_money.transfer(source_acctid, target_acctid, money)
    except Exception as e:
        print(str(e))
    finally:
        conn.close()
                            
         