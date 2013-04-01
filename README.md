Data_Warehouse_2
================

Data Warehouse repository for Software Engineering

To install mysql go to the  [download package](http://sourceforge.net/projects/mysql-python/).
Chris used [this tutorial](http://www.tutorialspoint.com/python/python_database_access.htm)
to get it to work.

How do I install the MySQLdb?
If it produces following result then it means MySQLdb module is not installed:

    Traceback (most recent call last):
      File "test.py", line 3, in <module>
        import MySQLdb
    ImportError: No module named MySQLdb

To install MySQLdb module, download it from MySQLdb Download page(first link) and proceed as follows:

    $ gunzip MySQL-python-1.2.2.tar.gz
    $ tar -xvf MySQL-python-1.2.2.tar
    $ cd MySQL-python-1.2.2
    $ python setup.py build
    $ python setup.py install

then run `python Warehouse_Consolidate.py`

git stuff
==========

    $ git clone https://github.com/drobin03/Data_Warehouse_2.git

what I do to push my stuff:

    $ git pull
    $ git add Warehouse_Consolidate.py # where Wharehous_Consolidate.py is the file you are working on
    $ git commit -m "nice message that says what u did"
    $ git push

