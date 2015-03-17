import threading
import sqlite3
import time

class Db_Controller(threading.Thread):
	
    def __init__(self, threadID, TFBSQ, EXP_TFBSQ, exitFlag, ft):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.TFBSQ = TFBSQ
        self.EXP_TFBSQ = EXP_TFBSQ
        self.con= self.set_con_db()
        self.cursor = self.con.cursor()
        self.exit_flag = exitFlag
        self.ft = ft
        self.unexp_genes = [] #explored genes list
        self.set_genes = set() #control set
        print "acaba el init"
        
    def run(self):
        print "Starting " + self.threadID
        self.process_data()
        print "Exiting " + self.threadID

    #Set a connection with the db
    def set_con_db(self):
        #return sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')
        print "conexion a base"
        con = sqlite3.connect('/home/daniel/Documents/fantom_db/genes.db', check_same_thread = False)
        return con
    
    def init_db(self):
        create_tables()
        self.add_row_GENES(self,"5558263","SRF")
        time.sleep(10)

    #Create the tables in the db, if they exists, then drop them
    def create_tables(self):
        print "create_tables"
        t1 = "GENES"
        t2 = "GENES_INTER"
        drop1 = "DROP TABLE IF EXISTS GENES;"
        drop2 = "DROP TABLE IF EXISTS GENES_INTER;"
        self.cursor.execute(drop1)
        self.con.commit()
        self.cursor.execute(drop2)
        self.con.commit()

        table1 = "CREATE TABLE GENES (ID INTEGER PRIMARY KEY AUTOINCREMENT, GENE_ID TEXT NOT NULL,GENE_SYMBOL TEXT NOT NULL);"
        self.cursor.execute(table1)
        self.con.commit()
        print "Se creo la tabla GENES correctamente"
        table2 = "CREATE TABLE GENES_INTER (ID INTEGER PRIMARY KEY AUTOINCREMENT, GENE1 INTEGER NOT NULL, GENE2 INTEGER NOT NULL, WEIGHT TEXT NOT NULL, FOREIGN KEY(GENE1) REFERENCES GENES(ID) ON DELETE CASCADE, FOREIGN KEY(GENE2) REFERENCES GENES(ID) ON DELETE CASCADE);"
        self.cursor.execute(table2)
        self.con.commit()
        print "Se creo la tabla GENES_INTER correctamente"        

    #Control data flow between the queues and the db
    def process_data(self):
        #first create tables 
        self.init_db(self)
        while not self.exit_flag:
            #check first the EXP_TFBS queue to put TFBS in the DB
            if not self.EXP_TFBSQ.empty():
		        print "paso por el primer if"
		        self.add_TFBS2DB()
            #then put news TFBS to explore in TFBSQ
            self.get_new_targets()
            #if this condition is true then, the program had explored all genes in FANTOM4 edge db
            if self.TFBSQ.empty() and self.EXP_TFBSQ.empty():
                print "paso por el segundo if"                
                self.exit_flag = 1

    #Add the gene and its interaction in the db 
    def add_TFBS2DB(self):
        size_q = self.EXP_TFBSQ.qsize()
        print "size ",size_q
        while size_q > 0:
            gene_raw = self.EXP_TFBSQ.get()
            print "en add_TFBS2DB {0}".format(gene_raw)
            add_rows(self,gene_raw)
            size_q-=1
            print "size reducido ", size_q

    def add_rows(self,gene_raw)
        try:
            #if the gene is not yet in the db, then add that entry to set (set_genes)            
            if not gene_raw[0] in self.set_genes:
                 gene_id = gene_raw[0]
                 gene_sym = gene_raw[1]
                 self.set_genes.add(gene_id)
                 self.unexp_genes.append((gene_sym,gene_id))
                 self.add_row_GENES(self,gene_id,gene_sym)
                 self.add_row_GENES_INTER(self,gene_raw):
            #if the gene is in, then check if the weight is greater than the others in db           
            else:
                gene1_q = self.query_id(gene_raw[0])
                gene2_q = self.query_id(gene_raw[4])
                self.update_weight(gene_raw[2],gene1_q,gene2_q,gene_raw[5])
        except TypeError:
            print "bla"

                
    #Make a query for the id in db with the gene name
    def query_id(self, gene):
        att = (gene,)
        query = "SELECT ID FROM GENES WHERE GENE_ID = ?;"
        self.cursor.execute(query, att)
        data=self.cursor.fetchone()
        if data is None:
            print 'There is no component named {0}'.format(gene)
            return 0 #necesito checar como cambiar esta parte
        else:
            print 'Component {0} found with rowid {1}'.format(gene,data[0])
            return data[0]

    #Add a the new genes to db 
    def add_row_GENES(self,gene_id,gene_sym):
        #insert in first table
        insert1 = "INSERT INTO GENES (ID, GENE_ID, GENE_SYMBOL) VALUES (NULL,?,?);"
        param1 = (gene_id,gene_sym)        
        self.con.execute(insert1,param1)
        self.con.commit() 

    #Add a the new interaction to db
    def add_row_GENES_INTER(self,gene):
        #insert in second table
        insert2 = "INSERT INTO GENES_INTER (ID, GENE1, GENE2, WEIGHT) VALUES (NULL,?,?,?);"
        q1 = self.query_id(gene[0]) 
        q2 = self.query_id(gene[4])
        if gene[5] == '0':          
            param2 = (q1,q2,gene[2])  
        else:
            param2 = (q2,q1,gene[2])
  
        self.con.execute(insert2,param2)
        self.con.commit()


    #Check the most high weight between all targets in/out, if the new weight is greater, change the weight with the new one
    def update_weight(self,g_w,gene1,gene2,in_out):
        if in_out == '0':   
            att = (gene1,gene2)
        else:
            att = (gene2,gene1)
        query = "SELECT ID, WEIGHT FROM GENES_INTER WHERE GENE1 = ? AND GENE2 = ?;"
        self.cursor.execute(query, att) 
        data=self.cursor.fetchone()
        if data is None:
	        print('There is no row with that genes')
        else:
            if float(g_w) < float(data[0]):
                self.update_row(data[0],data[1]) 

	#Update the weight of an interaction
    def update_row(self,id_row, weight):
        att = (weight, id_row)
        update = "UPDATE GENES_INTER SET WEIGHT = ? WHERE ID = ?;"
        self.cursor.execute(update, att)
        self.con.commit()

    #put all unxplore genes of unexp_genes list in EXP_TFBSQ queue for consumer threads
    def get_new_targets(self):
        print "paso por new targets"
        if len(self.unexp_genes) == 0:
            print "esta vacio el queue"
        else:
            print "el tamano de la lista es ", len(self.unexp_genes)
            print self.unexp_genes 
            for g in self.unexp_genes:
                a = self.EXP_TFBSQ.put(g[1])
                self.unexp_genes.remove(g)
        
########################END CLASS Db_Controller########################


