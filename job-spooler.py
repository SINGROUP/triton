import os
import math
import random
import numpy
import fnmatch
import sys
import glob
import time
import commands



# Read the comments to find out where to edit.
# This example works with ORCA jobs.



# set the name of the file with the list of
# completed jobs
file_done = "completed.txt"

# max amount of concurrent jobs in the triton queue
maxjobs = 2000

# list of currently running jobs - initialised to empty
actives = []
activesID = []

# Pool of IDs for the jobs. Just a list of integers.
# When a job starts, it pulls an ID from here.
# When it finishes, the ID returns to the pool.
runIDs = set(range(maxjobs))


# waiting time between spooler checks in seconds.
# keep this high so that the spooler will not appear to the admins
# as a CPU sucking process that some wanker left on the login node.
waittime = 60


# Prefix for job names. All jobs in the SLURM queue that starts with this
# will be considered spooler jobs.
jnamebase = 'jm'



## Builds the complete list of jobs.
# It also runs a completion check on all jobs found.
# 
# Returns two set objects: not-completed and completed jobs.
# The sets are effectively lists of input files (strings).
#
def GetJobList():

        # this examples uses glob to find all files with a certain
        # name pattern in the current folder
        jobs = set(glob.glob("mol*.in"))

        # --- do not edit this function after this point --- #
        
	notdone = []
	done = []

	fc = open(file_done,"w")
	for m in jobs:
		result = CheckCompleted(m)
		if result[0] == False and result[1] == True:
			notdone.append(m)
		elif result[0] == True:
			done.append(m)
			fc.write(m+'\n')
	
	fc.close()

        # make the lists sets so that doubles go away!
	notdone = set(notdone)
	done = set(done)

	print 'job list parsed. completed:',len(done),'to run:',len(notdone)
	return notdone, done



## Get the list (set) of completed jobs from the file.
# No need to change this function.
#
def RunsDone():
	fc = open(file_done, "r")
	done = []
	for l in fc.readlines():
		line = l.strip('\n')
		w = line.split()
		if len(w)>1: line = w[0]
		if len(line) > 0: done.append(line)
	done = set(done)
	fc.close()
	return done



## Creates a job for inputfile fname and submits it to the queue.
# Also adds an entry to active job list.
# In this case the function creates a batch job file for SLURM,
# and submit it.
#
# If the resubmit flag is true, the job file will not be recreated,
# just resubmitted (default=False).
# 
# All of this function should be edited to fit your needs!.
#
def MakeJob(fname, resubmit=False):

	jobfile = fname+".job"
	
	if not resubmit:

                # --- EDIT FROM HERE --- #
                
		os.system("cp trimer.job "+jobfile) #clone the file
		
                # uses console commands to edit a template .job file with sed
                # replace this code with what suits you best!

		cmd = "sed -i \'s/DIMERQUEUE/short/g\' ./" + jobfile
		os.system(cmd)
		
		cmd = "sed -i \'s/DIMERIN/"+fname+"/g\' ./" + jobfile
		os.system(cmd)
		cmd = "sed -i \'s/DIMEROUT/"+fname+".out"+"/g\' ./" + jobfile
		os.system(cmd)
		cmd = "sed -i \'s/DIMERTIME/00:30:00/g\' ./" + jobfile
		os.system(cmd)
		
                # gets an ID for this run from the pool
		jid = runIDs.pop() # pull the first free ID and remove it

		actives.append(fname) # add to the actives
		activesID.append([jid,fname]) # add a pair [ID, inputfile] to a global list of active jobs
		
                # set the job name for SLURM
		cmd = "sed -i \'s/DIMERNAME/"+jnamebase+"_"+str(jid)+"/g\' ./" + jobfile
		os.system(cmd)


                # --- DO NOT EDIT PAST THIS! ---#

		# write it in a log file in case something happens - it shouldnt!
		fjobs = open("submitted.jobs","a")
		fjobs.write(fname+" "+str(jid)+"\n")
		fjobs.close()
                
		print "spooler: submitting job",fname,jid
	else:
                # resubmitted jobs do not get a new ID, they keep the same
		print "spooler: re-submitting job",fname
	
	# queue the job
	os.system("sbatch "+jobfile)
	
	return



## This functions check if a job is completed or needs rerun.
# The job is identified by its input file name, fname.
# Change this according to your calculation output.
#
# The function should return two boolean values, indicating whether the job
# was completed and if the spooler should requeue it.
#
# Returns x, y where x is BOOL for complete/not-completed
# and y is BOOL for retry/leave-it-be
#
def CheckCompleted(fname):

        # name of the job file
	jobfile = fname+".job"

        # name of the output file
	outfile = fname+".out"
	
        iscomplete = False
        retry = False

        
        # runs a set of shell commands to find certain lines in the output files
        # depending on the response, determine whether the job never ran, crashed or finished.
        # adjust this code to fit your needs!
        #
        
	result = commands.getstatusoutput('grep \'There is no basis function on atom\' ' + outfile)
	if result[0] == 0:
		print fname, 'missing basis!'
		return iscomplete, retry
	
	result = commands.getstatusoutput('grep \' TERMINATED NORMALLY\' ' + outfile)
	if result[0] != 0:
		print fname, 'not terminated!'
                retry = True
		return iscomplete, retry
	
	result = commands.getstatusoutput('grep \'SCF CONVERGED AFTER\' ' + outfile)
	if result[0] != 0:
		print fname, 'not scf converged!'
                retry = True
		return incomplete, retry
	
	#code here means that the scf was converged and the run terminated normally!
	
	print fname, 'completed'
        iscomplete = True

        # --- edit all of this function as needed --- #

	return iscomplete, retry



# Marks a job as completed.
# No need to edit this.
# 
def MarkCompleted(fname, jobID, failed=False):

	print 'markcompleted',fname,jobID
	#print 'actives are:',actives
	#print 'activesID are:',activesID
	
	fc = open("completed.txt","a")
	fc.write(fname)
	if failed: fc.write(' [not working!]')
	fc.write('\n')
	fc.close()
	#remove the ID from the active set
	runIDs.add(jobID) #give back to index pool
	actives.remove(fname)
	activesID.remove([jobID,fname])

	#print 'actives are NOW:',actives
	#print 'activesID are NOW:',activesID


# Checks the queue for jobs belonging to the spooler.
# No modification to this functions needed.
# Returns a set of job IDs for the queued jobs.
#
def RunningJobs(): 

        # parse the squeue output...
	os.system("squeue -u YOUR-USER-NAME-HERE | grep '"+jnamebase+"_' > queue.info") #print the queue
	fq = open("queue.info","r")
	lines = fq.readlines()
	fq.close()
	
	ons = []
	for l in lines:
		job = l.split()[2]
		job = int(job.split('_')[1])
		ons.append(job)
		#print "running job", job
	
	print "spooler: # jobs running", len(lines)
	print "spooler: # jobs that were submitted", len(activesID)

	ons = set(ons)
	return ons


## Main function.
#
def main():
	
	#get all molecules in that folder
        jobs, done = GetJobList()

        #TODO: check for currently running jobs and rebuild the actives and activesID
	
	# keep looping until all jobs are done, or a triton admin finds out about this!
	while True:
                
                # check (again) for completed jobs from the list file_done
		done = RunsDone()
                # get the set of jobs still to do
		todo = jobs - done - set(actives)
                
                print 'spooler loop:  jobs left:',len(todo), 'active slots:',len(actives), 'free slots:',len(runIDs)
                
                # if there are free slots in the queue and jobs left to do...
		if len(runIDs) > 0 and len(todo) > 0:
			
			#time to do a new run
			
			#read the completed runs - again - why?
			done = RunsDone()
                        # recompute list of jobs left to do - this seems quite redundant!
			todo = mols-done-set(actives)
			
			if len(mols-done) == 0: #all are done, no queued -> finish!
				print "spooler: all jobs finished, stopping spooler."
				break
			if len(todo) == 0: #all are done or queued... nothing to do
				time.sleep(waittime)
				continue
			
                        # if the code reaches here, then there are some jobs left that are not
                        # already running...
			# pick one from the todo list and start it
			fname = todo.pop()
                        # make the job files and submit
			MakeJob(fname)
			
                        # go back to the beginning of the loop
			continue
		# ------------------------------------------------------------

		# code here means no available queue slots
		if len(actives) == 0 and len(todo) == 0:
			print "spooler: all runs are done!"
			break
                
                # just wait
		time.sleep(waittime)
		
		# check if some runs finished
		ons = RunningJobs() # list of job IDs still active
		endedID = set([x[0] for x in activesID])-ons #list of indexes of ended runs
		endedFile = [ [x[0],x[1]] for x in activesID if x[0] in endedID]
		
		#print "spooler: finished IDs",endedID
		#print "spooler: ended jobs",endedFile
		
		# if there are finished runs, check that they did not fail
		# if failed, resubmit
		# if completed, add to completed list
                # check each ended job
		for run in endedFile:
			# check if it was really completed
			isdone, redo = CheckCompleted(run[1])
			if isdone == True:
				# record this as completed
				MarkCompleted(run[1], run[0]) #0 is the ID, 1 is the filename
                                
			else: #if the run was not successful
				if redo == True: #resubmit flag
					MakeJob(run[1], resubmit=True)
				else:
                                        # in this case the job failed, but CheckCompleted says theres no point retrying
                                        # maybe the input file was too messed up. mark it as done so we can move on with our lives!
					MarkCompleted(run[1], run[0], failed=True)
	
	
	#end of infinite loop (lol)
	print "spooler: loop ended."


if __name__ == "__main__":
	main()
