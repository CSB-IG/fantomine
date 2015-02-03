import threading
import sqlite3

class Db_Controller(threading.Thread):
	def __init__(self, threadID, TFBSQ, EXP_TFBSQ, exitFlag):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.TFBSQ = TFBSQ
		self.EXP_TFBSQ = EXP_TFBSQ
		self.conection = set_con_db()
		self.cursor = conection.cursor()
        self.exit_flag = exitFlag
        self.exp_genes = []
        

	def run(self):
		print "Starting " + self.name
		process_data()
		print "Exiting " + self.name

	def set_con_db():
		return sqlite3.connect('/Users/daniel/Desktop/INMEGEN/genes.bd')

	def process_data():		
		while not self.exitFlag:
            #check first the EXP_TFBS queue to put TFBS in the DB
            if not self.EXP_TFBSQ.empty():
                size_q = self.EXP_TFBSQ.size()
                add_TFBS2DB(size_q)
            #then put news TFBS to explore in TFBSQ
            get_new_targets()

            #if this condition is true then, the program had explored all genes in FANTOM4 edge db
            if self.TFBSQ.empty() and self.EXP_TFBSQ.empty():
                self.exit_flag = 1




    #aqui checamos en diccionario si ya esta el gen, si hay uno mejor y si si lo cambiamos 
    def add_TFBS2DB(size):
        while not size == 0:
            if 

	def add_row()

    #metodo no seguro 
	def delete_row()

	#method were we check the most high weight between all targets in/out
	def update_row()


########################END CLASS Db_Controller########################


