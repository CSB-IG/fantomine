import urllib2
from lxml import etree
import threading


class Url_Id_Consumer(threading.Thread):
    
	def __init__(self, threadID, TFBSQ, EXP_TFBSQ):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.TFBSQ = TFBSQ
		self.EXP_TFBSQ = EXP_TFBSQ


	def run(self):
		print "Starting " + self.threadID
		process_gene(self.threadID, self.TFBSQ, self.EXP_TFBSQ)
		print "Exiting " + self.threadID


	#Put the new TFBS in the ex_Q through exploring genes children in t_Q
	def process_genes(threadName, t_Q, ex_Q):
		while not exitFlag_Consumers: 
			if not t_Q.empty():
				id_gene = t_Q.get()
				print "%s processing %s" % (threadName, id_gene) 
				#calling crawler
				extract_data(id_gene, ex_Q)
			else:
				time.sleep(1)

    #search the genes in the xml pages of fantom db edge expression through http request 
	def extract_data(id_gene, ex_Q):
		req = urllib2.Request('http://fantom.gsc.riken.jp/4/edgeexpress/cgi/edgeexpress.fcgi?id='+id_gene)
        response = urllib2.urlopen(req)
        the_page = response.read()
	    tree = etree.XML(the_page)
	    #call the extract methods
	    extract_input_promoters(tree, ex_Q,id_gene)
	    extract_out_promoters(tree, ex_Q,id_gene)
		

	#extract promoters of the next gene in the queue
	def extract_input_promoters(tree, ex_Q,id_gene):
		#INPUT PROMOTERS_FROM_EDGE
		print "promoter_from_edges"
        input_pro = tree.findall('promoters/promoter_from_edges')
        for i in input_pro:
            id_gene = i.findall('link_from')
            for g in id_gene:	
		        ex_Q.put((g.attrib['feature_id'],g.attrib['name'],g.attrib['weight'],id_gene,'0'))
				print "en cola: " + g.attrib['feature_id'] +' '+ g.attrib['name'] +' '+ g.attrib['weight'] + ' ' + '0'

    
    #extract targets of the next gene in the queue
	def extract_out_promoters(tree, ex_Q,id_gene):
		#OUTPUT PROMOTERS_TO_EDGE
		print "\n\npromoter_to_edges"
        output_pro = tree.findall('tfbs_predictions/link_to')
        for o in output_pro:
	        ex_Q.put((o.attrib['feature_id'],o.attrib['name'],o.attrib['weight'],id_gene,'1'))
			print "en cola: " + o.attrib['feature_id'] +' '+ o.attrib['name'] +' '+ o.attrib['weight'] + ' ' + '1'


########################END CLASS Url_Id_Consumer########################
