import time
import json
from pprint import pprint

TIME_SCHED=500
JOB_CPU={"logistic_regression": 50,
	"k-means": 40,
	"sigmoid":30}

JOB_FILE={"logistic_regression":"log_curve.json",
	"k-means": "tanh.json",
	"sigmoid":"sigmoid_curve.json"}

class Job:
    counter=0
    def __init__(self,idnumber, name):
	print "starting job", name
        self.allotted_cpus=0
#        counter = counter+1
        self.id = idnumber
	self.iteration_no=1
	self.name = name
	self.accuracy = 0
	self.killed = False

    def get_gain_value_in_last_span(self, is_first=False):
        '''gain is dependent on our knowledge of how many iterations will run in next 500 sec and the no of cpus allotted'''
        #readfile and load into a dict - in 0.5 sec  file format job#:{"cpu":"", "iteration_no":"" 
        data = json.load(open(JOB_FILE[self.name]))
	cpus = JOB_CPU[self.name]
	found = False
	for value in data:
		#print value
		if is_first == True:
	                time_for_iteration = value["time_in_ms"]
			found=True
			break
		if value["iteration"] == self.iteration_no:
			gain = value["gain"]
			time_for_iteration = value["time_in_ms"]
			found = True
			break
	if found == False:
		'''all iteratons info is over , exit the program'''
		return False,False 
	iterations_completed = TIME_SCHED/time_for_iteration
	iterations_completed = (iterations_completed*self.allotted_cpus)/cpus
	print "iterations completed", iterations_completed
	last_gain =0
	accuracy = None
	for value in data:
		if value["iteration"] < int(self.iteration_no+iterations_completed) and value["iteration"] >= self.iteration_no :
			last_gain +=value["gain"]
			accuracy = value["accuracy"]
			print "setting accuracy", value
	if accuracy:
		self.accuracy = accuracy
	self.iteration_no +=int(iterations_completed)
	print "gain  in the last epoch is ", last_gain
	#print "%d : %f", %(iterations_completed, last_gain)
	return last_gain,True	


class Driver:
    def __init__(self):
        self.total_cpu = 200
        self.allocation={}

    def statically_allocate_resources(self,jobs,is_first=False):
	number_of_jobs = len(jobs)
        cpu_allot_dict={}
        total_gain =0
	to_be_exited=[]
        for job in jobs:
	    gain,found=job.get_gain_value_in_last_span(is_first=is_first)
	    if found == False:
		job.killed=True
		self.allocation[job.id] = 0
		print "killed job", job.id
		to_be_exited.append(job)
		continue
            cpu_allot_dict[job.id]=gain
	    #print "gain value for job %s is %f"%(job.name, gain)
            total_gain+= gain
	for x in to_be_exited:
		jobs.remove(x)
	print "total gain", total_gain
#	print total_gain
	for job in jobs:
#		pass
    		print "accuracy of the job %s is %f" % (job.name, job.accuracy) 
        return cpu_allot_dict

    def allot_resources(self,jobs,is_first=False):
        number_of_jobs = len(jobs)
        cpu_allot_dict={}
        total_gain =0
	to_be_exited=[]
        for job in jobs:
	    gain,found=job.get_gain_value_in_last_span(is_first=is_first)
	    if found == False:
		job.killed=True
		self.allocation[job.id] = 0
		print "killed job", job.id
		to_be_exited.append(job)
		continue
            cpu_allot_dict[job.id]=gain
	    #print "gain value for job %s is %f"%(job.name, gain)
	    
            total_gain+= gain
	for x in to_be_exited:
		jobs.remove(x)
	print "total gain", total_gain
#	print total_gain
	total_cpu = self.total_cpu
	for job in jobs:
	    if cpu_allot_dict[job.id]<=0 :
                self.allocation[job.id]= 10
		job.allotted_cpus = self.allocation[job.id]
                total_cpu = total_cpu-10
        for job in jobs:
	# if gain is 0, allot minimum of 10 cores
	    if total_gain and cpu_allot_dict[job.id]!=0:
            	self.allocation[job.id]= (cpu_allot_dict[job.id]/total_gain)*total_cpu
            job.allotted_cpus = self.allocation[job.id]
	    print "accuracy of the job %s is %f" % (job.name, job.accuracy) 
        return cpu_allot_dict

    def schedule_jobs_allocate(self, jobs, method):
	maxlimit = 30
	if method == "slaq":
		print "running slaq"
		for i in range(maxlimit):
	#	    if i==0:
	#		is_first=True
		    newallocation = self.allot_resources(jobs,is_first= not i)
		    #time.sleep(2)
		    print "allocation is", self.allocation
	elif method == "static":
		print "running fair sched"
		for i in range(maxlimit):
			self.statically_allocate_resources(jobs, is_first = not i)
			#time.sleep(2)

    def start_jobs(self,jobs,method):
        no_of_jobs = len(jobs)
        cpuperjob = int(self.total_cpu/no_of_jobs)
        for job in jobs:
            self.allocation[job.id] = cpuperjob
	    job.allotted_cpus = cpuperjob
        curr_time = time.time()
	self.schedule_jobs_allocate(jobs, method)       


if __name__ == '__main__':
    driver = Driver()
    jobs=[]
    name=["logistic_regression","k-means","sigmoid"]
    for i in range(0,3):
        job = Job(i,name[i])
        jobs.append(job)
    driver.start_jobs(jobs,"slaq")
    static_jobs=[]
    for i in range(6,9):
	job = Job(i,name[i-6])
        static_jobs.append(job)
    driver.start_jobs(static_jobs,"static")
