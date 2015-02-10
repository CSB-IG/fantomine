import threading
import sqlite3

class Db_Controller(threading.Thread):
	def __init__(self, threadID, TFBSQ, EXP_TFBSQ, exitFlag):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.TFBSQ = TFBSQ
		self.EXP_TFBSQ = EXP_TFBSQ
		self.con= set_con_db()
		self.cursor = conection.cursor()
        self.exit_flag = exitFlag
        self.exp_genes = []
        

	def run(self):
		print "Starting " + self.name
		process_data()
		print "Exiting " + self.name

    #Set a connection with the db
	def set_con_db():
		#return sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')
        return sqlite3.connect('/Users/daniel/Desktop/trabajo/fantomine/db/genes.db')
    
    
    def create_tables():
        
                
    #Control data flow between the queues and the db
	def process_data():
        create_tables()	
		while not self.exitFlag:
            #check first the EXP_TFBS queue to put TFBS in the DB
            if not self.EXP_TFBSQ.empty():
                add_TFBS2DB()
            #then put news TFBS to explore in TFBSQ
            get_new_targets()
            #if this condition is true then, the program had explored all genes in FANTOM4 edge db
            if self.TFBSQ.empty() and self.EXP_TFBSQ.empty():
                self.exit_flag = 1


    #Add the gene and its interaction in the db 
    def add_TFBS2DB():
        size_q = self.EXP_TFBSQ.size()
        while not size_q == 0:
            gene_raw = self.EXP_TFBSQ.get()
            #if the gene is not yet in the db, then add that entry            
            if not gene_raw[0] in self.exp_genes: 
                self.exp_genes[gene_raw[0]] = gene_raw
                add_row(gene_raw)
            #if the gene is in, then check if the weight is greater than the others in db           
            else:
                g_w = gene_raw[2]
                q_w = query_weight(gene_raw):
                if q_w < g_w:                    
                    update_row(g_w)

    
    def query_id(gene):
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


	def add_row(gene):
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
        q1 = query_id(gene_piv)
        q2 = query_id(gene_id)
        if gene[4] == '0':          
            param2 = (q2,q1,gene_weight)  
        else:
            param2 = (q1,q2,gene_weight)
  
        self.con.execute(insert2,param2)
        self.con.commit()
    

    def query_weight(gene):   
        att = (query_id(gene[1]),query_id(gene[3]))
        query = "SELECT WEIGHT FROM GENES_INTER WHERE GENE1 = ? AND GENE2 = ?;"
        self.cursor.execute(query, att)
        data=self.cursor.fetchone()
        if data is None:
	        print('There is no row')
            return -1000.0 
        else:
	        return float(data[0]) 


	#method were we check the most high weight between all targets in/out
	def update_row(id_row, weight): #MODIFICAR FALTA HACER EL QUERY DE LOS IDS DE LOS GENES PARA OBTENER EL ID DE LA TABLA GENES_INTER 
        att = (weight, id_gene)
        self.update = "UPDATE GENES_INTER SET WEIGHT = ? WHERE ID = ?;"
        self.con.commit()

########################END CLASS Db_Controller########################


