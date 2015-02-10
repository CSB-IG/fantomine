from consumer import Url_Id_Consumer
from db_control import Db_Controller
import Queue

def main():

	#name of threads
	threadList = []
	#max number of threads
	MAX_NUM_T = 27
	#queue of new TFBS to explore
	TFBSQ = Queue.Queue()
	#queue of explored TFBS with interactions and weights
	EXP_TFBSQ = Queue.Queue()
    #exit flag for db_thread
    exitFlag = 0 
    #exit flag for consumers
    exitFlag_Consumers = 0  
    #a gene for begin to explore
    TFBSQ.append('5558263')

	#enum the consumerthreads
	for i in range(MAX_NUM_T):
		threadList.append("Thread #"+str(i))
	print "all threads enum" 


	#create Url_Id_Consumer threads
	for j in range(MAX_NUM_T):
		thread = Url_Id_Consumer(threadID, TFBS, EXP_TFBSQ, exitFlag_Consumers)
		thread.start()
		threads.append(thread)

    #create Db_Controller thread
	db_thread = Db_Controller("Db_thread", TFBS, X_TFBS, exitFlag)    

	#while not TFBS.empty() and EXP_TFBS.empty():
    while not exitFlag:
	pass

	exitFlag_Consumers = 1

	#Wait for all threads to complete
	for t in threads:#
		t.join()#
    db_thread.join()

	print "Exiting Main Thread, DATA MINING COMPLETE jeje"
    	

if __name__ == '__main__':
    main()

