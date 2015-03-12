#-*- coding: ISO-8859-1 -*-
# pysqlite2/test/transactions.py: tests transactions
#
# Copyright (C) 2005-2009 Gerhard Häring <gh@ghaering.de>
#
# This file is part of pysqlite.
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.

import sys
import os, unittest
import pysqlite2.dbapi2 as sqlite

def get_db_path():
    return "sqlite_testdb"

class TransactionTests(unittest.TestCase):
    def setUp(self):
        try:
            os.remove(get_db_path())
        except OSError:
            pass

        self.con1 = sqlite.connect(get_db_path(), timeout=0.1)
        self.cur1 = self.con1.cursor()

        self.con2 = sqlite.connect(get_db_path(), timeout=0.1)
        self.cur2 = self.con2.cursor()

    def tearDown(self):
        self.cur1.close()
        self.con1.close()

        self.cur2.close()
        self.con2.close()

        try:
            os.unlink(get_db_path())
        except OSError:
            pass

    def CheckHasActiveTransaction(self):
        """Test that in_transaction returns the actual transaction state."""
        self.assertFalse(self.con1.in_transaction)
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.assertTrue(self.con1.in_transaction)
        self.con1.commit()
        self.assertFalse(self.con1.in_transaction)

        # Manage the transaction state manually and check if it is detected correctly.
        self.con2.isolation_level = None
        self.assertFalse(self.con2.in_transaction)
        self.cur2.execute("begin")
        self.assertTrue(self.con2.in_transaction)
        self.con2.commit()
        self.assertFalse(self.con2.in_transaction)

        self.cur2.execute("begin")
        self.assertTrue(self.con2.in_transaction)
        self.cur2.execute("commit")
        self.assertFalse(self.con2.in_transaction)


    def CheckDMLdoesAutoCommitBefore(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.cur1.execute("create table test2(j)")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)

    def CheckInsertStartsTransaction(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 0)

    def CheckUpdateStartsTransaction(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.con1.commit()
        self.cur1.execute("update test set i=6")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchone()[0]
        self.assertEqual(res, 5)

    def CheckDeleteStartsTransaction(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.con1.commit()
        self.cur1.execute("delete from test")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)

    def CheckReplaceStartsTransaction(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.con1.commit()
        self.cur1.execute("replace into test(i) values (6)")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], 5)

    def CheckToggleAutoCommit(self):
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        self.con1.isolation_level = None
        self.assertEqual(self.con1.isolation_level, None)
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)

        self.con1.isolation_level = "DEFERRED"
        self.assertEqual(self.con1.isolation_level , "DEFERRED")
        self.cur1.execute("insert into test(i) values (5)")
        self.cur2.execute("select i from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)

    def CheckRaiseTimeout(self):
        if sqlite.sqlite_version_info < (3, 2, 2):
            # This will fail (hang) on earlier versions of sqlite.
            # Determine exact version it was fixed. 3.2.1 hangs.
            return
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        try:
            self.cur2.execute("insert into test(i) values (5)")
            self.fail("should have raised an OperationalError")
        except sqlite.OperationalError:
            pass
        except:
            self.fail("should have raised an OperationalError")

    def CheckLocking(self):
        """
        This tests the improved concurrency with pysqlite 2.3.4. You needed
        to roll back con2 before you could commit con1.
        """
        if sqlite.sqlite_version_info < (3, 2, 2):
            # This will fail (hang) on earlier versions of sqlite.
            # Determine exact version it was fixed. 3.2.1 hangs.
            return
        self.cur1.execute("create table test(i)")
        self.cur1.execute("insert into test(i) values (5)")
        try:
            self.cur2.execute("insert into test(i) values (5)")
            self.fail("should have raised an OperationalError")
        except sqlite.OperationalError:
            pass
        except:
            self.fail("should have raised an OperationalError")
        # NO self.con2.rollback() HERE!!!
        self.con1.commit()

    def CheckRollbackCursorConsistency(self):
        """
        Checks if cursors on the connection are set into a "reset" state
        when a rollback is done on the connection.
        """
        con = sqlite.connect(":memory:")
        cur = con.cursor()
        cur.execute("create table test(x)")
        cur.execute("insert into test(x) values (5)")
        cur.execute("select 1 union select 2 union select 3")

        con.rollback()
        try:
            cur.fetchall()
            self.fail("InterfaceError should have been raised")
        except sqlite.InterfaceError, e:
            pass
        except:
            self.fail("InterfaceError should have been raised")

    def CheckDropTableRollback(self):
        """
        Checks that drop table can be run inside a transaction and will
        roll back correctly.
        """
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table test(x)")
        self.cur1.execute("insert into test(x) values (5)")
        self.con1.commit()
        self.cur1.execute("drop table test")
        self.con1.rollback()
        # Table should still exist.
        self.cur1.execute("select * from test")

    def CheckCreateTableRollback(self):
        """Checks that create table runs inside a transaction and can be rolled back."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table test(x)")
        self.con1.rollback()
        # Table test was rolled back so this should work
        self.cur1.execute("create table test(x)")

    def CheckSavepoints(self):
        """Trivial savepoint check."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table test(x)")
        self.con1.commit()
        self.cur1.execute("insert into test(x) values (1)")
        self.cur1.execute("savepoint foobar")
        self.cur1.execute("insert into test(x) values (2)")
        self.cur1.execute("rollback to savepoint foobar")
        self.con1.commit()
        self.cur2.execute("select x from test")
        res = self.cur2.fetchall()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], 1)

    def CheckCreateIndexRollback(self):
        """Check that create index is transactional."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table test(x integer)")
        self.cur1.execute("insert into test(x) values (1)")
        self.con1.commit()
        self.cur1.execute("create index myidx on test(x)")
        self.assertTrue(self.cur1.execute("pragma index_info(myidx)").fetchone())
        self.cur1.execute("insert into test(x) values (2)")
        self.con1.rollback()
        self.assertFalse(self.cur1.execute("pragma index_info(myidx)").fetchone())

    def CheckColumnAddRollback(self):
        """Check that adding a column is transactional."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table test(x integer)")
        self.cur1.execute("insert into test(x) values (42)")
        self.con1.commit()
        self.cur1.execute("alter table test add column y integer default 37")
        self.assertEqual(len(self.cur1.execute("select * from test").fetchone()), 2)
        self.con1.rollback()
        self.assertEqual(len(self.cur1.execute("select * from test").fetchone()), 1)
        try:
            self.cur1.execute("insert into test(x,y) values (1,2)")
            self.fail("Column y should have been rolled back.")
        except sqlite.OperationalError:
            pass

    def CheckTableRenameRollback(self):
        """Check that renaming a table is transactional."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table foo(x integer)")
        self.con1.commit()
        self.cur1.execute("alter table foo rename to bar")
        self.cur1.execute("select * from bar")
        try:
            self.cur1.execute("select * from foo")
            self.fail("Table foo should have been renamed to bar")
        except sqlite.OperationalError:
            pass
        self.con1.rollback()
        self.cur1.execute("select * from foo")
        try:
            self.cur1.execute("select * from bar")
            self.fail("Renaming the table should have been rolled back.")
        except sqlite.OperationalError:
            pass

    def CheckDropIndexRollback(self):
        """Check that dropping an index is transactional."""
        self.con1.operation_needs_transaction_callback = lambda x: True
        self.cur1.execute("create table foo(x integer)")
        self.cur1.execute("create index myidx on foo(x)")
        self.con1.commit()
        self.cur1.execute("drop index myidx")
        self.con1.rollback()
        try:
            self.cur1.execute("create index myidx on foo(x)")
            self.fail("Index myidx should exist here (dropping it was rolled back).")
        except sqlite.OperationalError as e:
            # OperationalError: index myidx already exists
            pass


class SpecialCommandTests(unittest.TestCase):
    def setUp(self):
        self.con = sqlite.connect(":memory:")
        self.cur = self.con.cursor()

    def CheckVacuum(self):
        self.cur.execute("create table test(i)")
        self.cur.execute("insert into test(i) values (5)")
        self.cur.execute("vacuum")

    def CheckDropTable(self):
        self.cur.execute("create table test(i)")
        self.cur.execute("insert into test(i) values (5)")
        self.cur.execute("drop table test")

    def CheckPragma(self):
        self.cur.execute("create table test(i)")
        self.cur.execute("insert into test(i) values (5)")
        self.cur.execute("pragma count_changes=1")

    def tearDown(self):
        self.cur.close()
        self.con.close()

def suite():
    default_suite = unittest.makeSuite(TransactionTests, "Check")
    special_command_suite = unittest.makeSuite(SpecialCommandTests, "Check")
    return unittest.TestSuite((default_suite, special_command_suite))

def test():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    test()
