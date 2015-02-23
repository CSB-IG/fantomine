import threading
import sqlite3

class Db_Controller(threading.Thread):
	
    def __init__(self, threadID, TFBSQ, EXP_TFBSQ, exitFlag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.TFBSQ = TFBSQ
        self.EXP_TFBSQ = EXP_TFBSQ
        self.con= self.set_con_db()
        self.cursor = self.con.cursor()
        self.exit_flag = exitFlag
        self.exp_genes = []
        print "acaba el init"
        

    def run(self):
        print "Starting " + self.name
        print "aqui"
        self.process_data()
        print "Exiting " + self.name

    #Set a connection with the db
    def set_con_db(self):
        #return sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')
        print "conexion a base"
        return sqlite3.connect('/home/daniel/Documents/fantom_db/genes.db')
    
    #Create the tables in the db, if they exists, then drop them
    def create_tables(self):
        print "create_tables"
        t1 = "GENES"
        t2 = "GENES_INTER"
        drop = "DROP TABLE IF EXISTS ?"
        self.cursor.execute(drop,(t1,))
        self.con.commit()
        self.cursor.execute(drop,(t2,))
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
    def process_data():
        #first create tables 
        self.create_tables()
        while not self.exitFlag:
            #check first the EXP_TFBS queue to put TFBS in the DB
            if not self.EXP_TFBSQ.empty():
                self.add_TFBS2DB()
            #then put news TFBS to explore in TFBSQ
            self.get_new_targets()
            #if this condition is true then, the program had explored all genes in FANTOM4 edge db
            if self.TFBSQ.empty() and self.EXP_TFBSQ.empty():
                self.exit_flag = 1


    #Add the gene and its interaction in the db 
    def add_TFBS2DB(self):
        size_q = self.EXP_TFBSQ.size()
        while not size_q == 0:
            gene_raw = self.EXP_TFBSQ.get()
            gene1_q = query_id(gene[1])
            gene2_q = query_id(gene[3])
            #if the gene is not yet in the db, then add that entry            
            if not gene_raw[0] in self.exp_genes: 
                self.exp_genes[gene_raw[0]] = gene_raw
                self.add_row(gene_raw)
            #if the gene is in, then check if the weight is greater than the others in db           
            else:
                g_w = gene_raw[2]
                self.update_weight(g_w,gene1_q,gene2_q)
                

    #Make a query for the id in db with the gene name
    def query_id(self, gene):
        att = (gene,)
        query = "SELECT ID FROM GENES WHERE GENE_ID = ?;"
        self.cursor.execute(query, att)
        data=cursor.fetchone()
        if data is None:
            print('There is no component named %s'%gene)
            return 0
        else:
            print('Component %s found with rowid %s'%(gene,data[0]))
            return data[0]

    #Add a the new genes and interactions to db
    def add_row(self,gene,q1,q2):
        gene_id = gene[0]
        gene_sym = gene[1]
        gene_w = gene[2]
        gene_piv = gene[3]    

        param1 = (gene_id,gene_sym)    

        #insert in first table
        insert1 = "INSERT INTO GENES (ID, GENE_ID, GENE_SYMBOL) VALUES (NULL,?,?);"
        self.con.execute(insert1,param)
        self.con.commit()
        
        #insert in second table
        insert2 = "INSERT INTO GENES_INTER (ID, GENE1, GENE2, WEIGHT) VALUES (NULL,?,?,?);"
        if gene[4] == '0':          
            param2 = (q1,q2,gene_weight)  
        else:
            param2 = (q2,q1,gene_weight)
  
        self.con.execute(insert2,param2)
        self.con.commit()
    
    #Check the most high weight between all targets in/out, if the new weight is greater, change the weight with the new one
    def update_weight(self,g_w,gene1,gene2):   
        att = (gene1,gene2)
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
        att = (weight, id_gene)
        update = "UPDATE GENES_INTER SET WEIGHT = ? WHERE ID = ?;"
        self.cursor.execute(update, att)
        self.con.commit()

########################END CLASS Db_Controller########################


