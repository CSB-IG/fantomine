import sqlite3

con = sqlite3.connect('/home/daniel/Desktop/trabajo/tesis_final/miscellaneous/genes.bd')
#con = sqlite3.connect('/Users/daniel/Desktop/genes.bd')
#con = sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')

#cursor = con.cursor()
cursor = con.cursor()

print "la base se abrio correctamente"


cursor.execute('''CREATE TABLE GENES (ID INTEGER PRIMARY KEY AUTOINCREMENT, GENE_ID TEXT NOT NULL,GENE_SYMBOL TEXT NOT NULL);''')
print "la tabla de genes se creo correctamente"


cursor.execute('''CREATE TABLE GENES_INTER (ID INTEGER PRIMARY KEY AUTOINCREMENT, GENE1 INTEGER NOT NULL, GENE2 INTEGER NOT NULL, WEIGHT TEXT NOT NULL, FOREIGN KEY(GENE1) REFERENCES GENES(ID) ON DELETE CASCADE, FOREIGN KEY(GENE2) REFERENCES GENES(ID) ON DELETE CASCADE);''')
print "la tabla de genes_inter se creo correctamente"

#cursor.execute('''CREATE TABLE DICT (ID INT PRIMARY KEY NOT NULL, GENE_ID	TEXT NOT NULL);''')
#print "la tabla de genes se creo correctamente"

