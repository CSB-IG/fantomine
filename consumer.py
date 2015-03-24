import urllib2
from lxml import etree
import threading
import time


class Url_Id_Consumer(threading.Thread):
    
    def __init__(self, threadID, TFBSQ, EXP_TFBSQ,exitFlag_Consumers):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.TFBSQ = TFBSQ
        self.EXP_TFBSQ = EXP_TFBSQ
        self.exitFlag = exitFlag_Consumers 


    def run(self):
        print "Starting " + self.threadID
        self.process_genes(self.threadID, self.TFBSQ, self.EXP_TFBSQ)
        print "Exiting " + self.threadID


    #Put the new TFBS in the ex_Q through exploring genes children in t_Q
    def process_genes(self, threadName, t_Q, ex_Q):
        while not self.exitFlag: 
            if not t_Q.empty():
                id_gene = t_Q.get() 
                #calling crawler
                self.extract_data(threadName, id_gene, ex_Q)
            else:
                #print "t_Q is empty\nthread {0} is sleeping".format(threadName) 
                time.sleep(1)

    #search the genes in the xml pages of fantom db edge expression through http request 
    def extract_data(self, threadName, id_gene, ex_Q):
        req = urllib2.Request('http://fantom.gsc.riken.jp/4/edgeexpress/cgi/edgeexpress.fcgi?id='+id_gene[1])
        response = urllib2.urlopen(req)
        the_page = response.read()
        tree = etree.XML(the_page)
        print "\n\n{0} processing {1}".format(threadName, id_gene)
	    #call the extract methods
        self.extract_input_promoters(tree, ex_Q,id_gene)
        self.extract_out_promoters(tree, ex_Q,id_gene)
		

	#extract promoters of the next gene in the queue
    def extract_input_promoters(self, tree, ex_Q, id_g):
        #INPUT PROMOTERS_FROM_EDGE
        print "PROMOTERS_FROM_EDGE"
        input_pro = tree.findall('promoters/promoter_from_edges')
        for i in input_pro:
            id_gene = i.findall('link_from')
            for g in id_gene:
                tup1 = (g.attrib['feature_id'],g.attrib['name'],g.attrib['weight'],id_g[0],id_g[1],'0')
                ex_Q.put(tup1)
                print "en cola: {0}, {1}, {2}, {3}, {4}, {5}".format(tup1[0],tup1[1],tup1[2],tup1[3],tup1[4],tup1[5])

    
    #extract targets of the next gene in the queue
    def extract_out_promoters(self, tree, ex_Q, id_g):
        #OUTPUT PROMOTERS_TO_EDGE
        print "\nPROMOTER TO EDGE"
        output_pro = tree.findall('tfbs_predictions/link_to')
        for o in output_pro:
            tup2 = (o.attrib['feature_id'],o.attrib['name'],o.attrib['weight'],id_g[0],id_g[1],'1')
            ex_Q.put(tup2)
            print "en cola: {0}, {1}, {2}, {3}, {4}, {5}".format(tup2[0],tup2[1],tup2[2],tup2[3],tup2[4],tup2[5])


########################END CLASS Url_Id_Consumer########################
