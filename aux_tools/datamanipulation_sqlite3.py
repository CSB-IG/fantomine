import sqlite3

#con = sqlite3.connect('/home/daniel/Desktop/trabajo/tesis_final/miscellaneous/genes.bd')
con = sqlite3.connect('/home/daniel/Documents/fantom_db/genes.db')
#con = sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')
#con esta opcion habilitamos el borrado en cascada
con.execute("PRAGMA foreign_keys = ON")
cursor = con.cursor()


print "la base se abrio correctamente"

'''
To insert the data you don't need a cursor

just use the con

con.execute() instead of c.execute() and get rid of the c = con.cursor() line

Cursors aren't used to insert data, but usually to read data, or update data in place.

'''
#para insertar en la primera tabla
'''
gene_id = "555558"
gene_sym = "p53"
gene_w = 2.07
gene_piv = "55555"

param = (gene_id,gene_sym)    

insert1 = "INSERT INTO GENES (ID, GENE_ID, GENE_SYMBOL) VALUES (NULL,?,?)"
con.execute(insert1,param)
print "se inserto correctamente en GENES"
#usamos commit para ver que movimientos se hicieron en la base de datos
con.commit() 
'''

#Mostrar elementos de la primera tabla

print "=======GENES======="
cursor.execute("SELECT ID, GENE_ID, GENE_SYMBOL FROM GENES")
for i in cursor:
	print "ID = ", i[0], "GENE_ID = ", i[1], "GENE_SYMBOL = ", i[2]


#para insertar en la segunda tabla
'''
insert2 = "INSERT INTO GENES_INTER (ID, GENE1, GENE2, WEIGHT) VALUES (NULL,?,?,?)"
param2 = (1,3,gene_w)
con.execute(insert2, param2)
con.execute(insert2, (4,3,9.5))
print "se inserto correctamente en GENES_INTER "
con.commit()
'''

#para borrar elementos
'''
id_pri = 1
cursor.execute("DELETE FROM GENES WHERE ID ="+ str(id_pri) +";") 

print "se borro el registro con ID = ", id_pri
con.commit()
'''

#Mostrar la existencia de un registro
'''
name = '555555'
att = (name,)
query = "SELECT ID FROM GENES WHERE GENE_ID = ?;"
cursor.execute(query, att)
data=cursor.fetchone()
if data is None:
	print('There is no component named %s'%name)
else:
	print('Component %s found with rowid %s'%(name,data[0]))
'''

#mostrar elementos de la segunda tabla

print "\n=======GENES_INTER======="
cursor.execute("SELECT * FROM GENES_INTER")
for i in cursor:
	print "ID = ", i[0], "GENE1 = ", i[1], "GENE2 = ", i[2], "WEIGHT = ", i[3]


#Mostrar la existencia de un registro 
'''
att = (4,3)
query = "SELECT * FROM GENES_INTER WHERE GENE1 = ? AND GENE2 = ?;"
cursor.execute(query, att)
data=cursor.fetchone()
if data is None:
	print('There is no component')
else:
	print('Component found %s'%(data[0]))
'''

#actualizar una fila en la base
'''
weight = "34.5"
id_gene = 1 
att = (weight, id_gene)
update = "UPDATE GENES_INTER SET WEIGHT = ? WHERE ID = ?;"
cursor.execute(update, att)
con.commit()
'''



