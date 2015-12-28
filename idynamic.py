#!/usr/bin/env python

import argparse
import sys
import pyslurm
import time
import os
import shlex
import subprocess
import signal

_DEFAULT_PROFILE = None
_DEFAULT_PROFILE_DIR = None
_DEFAULT_N_WANTED = 10
_DEFAULT_MAX_N_QUEUED = 20

_JOB_DESC = '''#!/usr/bin/env bash
#SBATCH --partition=short --qos=short
#SBATCH --time=4:00:00
#SBATCH --mem=17000
#SBATCH --mem-per-cpu=17000
#SBATCH -n 1
#SBATCH -c 1
#SBATCH -J dynamic
#SBATCH -o dynamic.engine-%J.out
#SBATCH --auks=yes

hostname=`uname -n`

echo "I AM: $hostname"
echo ""
echo "NODE LIST: $SLURM_JOB_NODELIST"
echo "AVAILABLE CORES: $SLURM_JOB_CPUS_PER_NODE"
'''

_JOB_COMMAND = 'mpiexec -n $SLURM_JOB_CPUS_PER_NODE ipengine'

_SUBMIT_COMMAND = 'sbatch %s'

def log(string = '') :
    fmt_time = time.strftime('%H:%M:%S', time.gmtime())
    print '[%s] %s' % (fmt_time, string)

class JobState :
    RUNNING, SUSPENDED, COMPLETED, TIMEOUT, COMPLETING, PENDING = 'RUNNING', 'SUSPENDED', 'COMPLETED', 'TIMEOUT', 'COMPLETING', 'PENDING'

class QueueManager(object) :

    def __init__(self, profile = _DEFAULT_PROFILE, profile_dir = _DEFAULT_PROFILE_DIR, n_wanted = _DEFAULT_N_WANTED, max_n_queued = _DEFAULT_MAX_N_QUEUED, verbose = 0) :
        self.profile = profile
        self.profile_dir = profile_dir
        self.n_wanted = n_wanted
        self.max_n_queued = max_n_queued
        self.verbose = verbose
        self.pid = os.getpgid(0)
        self.submitted_jobs = set()
   
    def infinite_poll(self, interval = 1) :
        '''
        An infinite poll loop.
        :param interval : polling interval (in seconds).
        '''
        try :
            while True :
                self.poll()
                time.sleep(interval)
        except KeyboardInterrupt :
            jobs = [job_id for job_id in self.submitted_jobs]
            n_jobs = len(jobs)
            log()
            if n_jobs > 0 :
                log('[!] Caught interrupt -> cancelling: %s' % ' '.join([str(job_id) for job_id in jobs]))
                for job_id in jobs :
                    self.kill_job(job_id)
            else :
                log('[!] Caught interrupt -> exiting')

    def poll(self) :
        '''
        Poll jobs, count engines, take appropriate action.
        '''
        running_ids, queued_ids, completed_ids, missing_ids = self.get_submitted_job_info()
        n_running = len(running_ids)
        n_queued = len(queued_ids)
        n_completed = len(completed_ids)
        n_missings = len(missing_ids)
        
        # Clean up IDs
        for job_id in completed_ids + missing_ids :
            self.kill_job(job_id)

        if len(completed_ids) > 0 :
            log('[i] Completed jobs: ' + ' '.join(str(id) for id in completed_ids))
        
        if len(missing_ids) > 0 :
            log('[!] Missing jobs: ' + ' '.join(str(id) for id in missing_ids))

        if n_running + n_queued < self.n_wanted and n_queued < self.max_n_queued :
            if n_queued < self.max_n_queued :
                n_submit = min(self.n_wanted - (n_running + n_queued), self.max_n_queued - n_queued)
                log('[i] Status: %d (running) + %d (queued) / %d (wanted) -> SUBMIT %d' % (n_running, n_queued, self.n_wanted, n_submit))
                for i in range(n_submit) :
                    job_id = self.submit_job()
                    if job_id > 0 :
                        log('    [+] %3d / %3d : Submitted job_id = %d' % (i + 1, n_submit, job_id))
                    else :
                        log('    [-] %3d / %3d : Submission failed' % (i + 1, n_submit))

            else :
                if self.verbose >= 1 :
                    log('[i] Status: %d (running) + %d (queued) / %d (wanted) -> WAIT (queued %d / %d)' % (n_running, n_queued, self.n_wanted, n_queued, self.max_n_queued))
        else :
            if self.verbose >= 2 :
                log('[i] Status: %d (running) + %d (queued) / %d (wanted)' % (n_running, n_queued, self.n_wanted))

    def submit_job(self) :
        '''
        Submits a new engine job and update the bookeeping list accordingly.
        '''
        command = _JOB_COMMAND
        if self.profile is not None :
            command += ' --profile=%s' % self.profile
        if self.profile_dir is not None :
            command += ' --profile-dir=%s' % self.profile_dir
        description = ''.join([_JOB_DESC, command, '\n'])

        filename = 'job_idynamic.%d' % self.pid
        QueueManager._write_job_description_file(description, filename)
        job_id = QueueManager.slurm_submit(filename)
        if job_id > 0 :
            self.submitted_jobs.add(job_id)
        return job_id

    def kill_job(self, job_id) :
        '''
        Kill a job and remove it from the list of submitted jobs.
        :param job_id : SLURM ID of the job to be killed.
        '''
        self.submitted_jobs.remove(job_id)
        result = True
        try :
            pyslurm.slurm_kill_job(job_id, Signal = 9, BatchFlag = 0)
        except :
            result = False
        return result

    def get_submitted_job_info(self) :
        '''
        Get information on submitted jobs.
        '''
        slurm_jobs = pyslurm.job()
        running_ids, queued_ids, completed_ids, missing_ids = [], [], [], []
        for job_id in self.submitted_jobs :
            info = slurm_jobs.find_id(job_id)
            if len(info) == 0 :
                missing_ids.append(job_id)
                continue

            if info['job_state'] in  [JobState.RUNNING, JobState.SUSPENDED] :
                running_ids.append(job_id)
            elif info['job_state'] == JobState.PENDING :
                queued_ids.append(job_id)
            elif info['job_state'] in [JobState.COMPLETED, JobState.COMPLETING, JobState.TIMEOUT] :
                completed_ids.append(job_id)
            else :
                log('[e] Unknown job state %s (job id = %d)' % (info['job_state'], job_id))

        return running_ids, queued_ids, completed_ids, missing_ids 

    @staticmethod
    def slurm_submit(filename) :
        '''
        Submit a job to SLURM.
        :param filename : name of the file to be submitted.
        '''
        slurm_command = _SUBMIT_COMMAND % filename
        slurm_command = shlex.split(slurm_command)
        job_id = -1
        try :
            proc = subprocess.Popen(slurm_command, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            fout, _ = proc.communicate()
            lines = fout.split('\n')
            line = lines[0]
            args = line.split()
            job_id = int(args[-1])
        except :
            pass

        return job_id
    
    @staticmethod
    def _write_job_description_file(job_desc, filename) :
        '''
        Write job description into a file with a given name.
        :param job_desc : job description text.
        :pram filename : name of the file to be written.
        '''
        fout = open(filename, 'wb')
        fout.write(job_desc)
        fout.close()

def get_args() :
    '''
    Get arguments.
    '''
    parser = argparse.ArgumentParser(description = 'Dynamic iPython cluster engine queueing.')
    parser.add_argument('--profile', type = str, dest = 'profile', nargs = 1, default = _DEFAULT_PROFILE, help = 'iPython profile name')
    parser.add_argument('--profile-dir', type = str, nargs = 1, dest = 'profile_dir', default = _DEFAULT_PROFILE_DIR, help = 'iPython profile directory')
    parser.add_argument('--n', type = int, nargs = 1, dest = 'n_wanted', default = _DEFAULT_N_WANTED, help = 'Desired number of running engines (default: n = %d)' % _DEFAULT_N_WANTED)
    parser.add_argument('--q', type = int, nargs = 1, dest = 'max_n_queued', default = _DEFAULT_MAX_N_QUEUED, help = 'Maximum number of queued jobs (default: q = %d)' % _DEFAULT_MAX_N_QUEUED)

    args = parser.parse_args()
    return args

def check_args(config) :
    '''
    Check configuration arguments.
    :param config : configuration to be checked.
    '''
    for property in ['profile', 'profile_dir', 'n_wanted', 'max_n_queued'] :
        value = config.__getattribute__(property)
        if isinstance(value, list) :
            config.__setattr__(property, value[0])

    if config.n_wanted <= 0 :
        log('[!] (Error) Number of wanted jobs cannot be smaller than 1.')
        exit(1)
    if config.max_n_queued <= 0 :
        log('[!] (Error) Number of queued jobs cannot be smaller than 1.')
        exit(1)

def main() :
    '''
    Main function.
    '''
    config = get_args()
    check_args(config)

    manager = QueueManager(config.profile, config.profile_dir, config.n_wanted, config.max_n_queued)
    log('[+] Started: pid = %d' % manager.pid)
    manager.infinite_poll(interval = 30)

if __name__ == '__main__' :
    main()
