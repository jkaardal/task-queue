import socket
import os
import subprocess
import paramiko
import getpass
import random
import string
import time

'''Submit jobs to a set of host machines via ssh; the jobs are run
within a self-terminating screen session. This script assumes that the
file system is preserved on each host across a network. If necessary,
the pre-submission script may be used to transfer files prior to job
initiation.'''

# when True, prints job submission commands but does not connect to
# hosts or submit jobs; when False submits jobs on hosts
test = False

# generate jobs (array where each element is a string of a command to
# be run on one of the hosts)
jobs = []
for i in range(6):
    jobs.append("python test_script.py")

# set hosts to try (array where each element is a string of the host
# name) in order of preference a semi-colon and then a
# semi-colon-delimited list of environment variable contingencies can
# be appended to the hostname
hosts = ["adelaide", "sydney; gpu0", "sydney; gpu1", "melbourne", "perth", "darwin"]

# define a dictionary of environment contingencies to inherit to the screen session
envcom = dict()
envcom['gpu0'] = "export USE_THIS_GPU=gpu0"
envcom['gpu1'] = "export USE_THIS_GPU=gpu1"

# if shared_file_system = True, default directory for screen session
# is script_path; if shared_file_directory = False, the default
# directory is the home folder
rundir = dict()
rundir['melbourne'] = "~/different_path"

# script internals
username = raw_input("username: ")
password = getpass.getpass(prompt="password: ")

script_path = os.path.dirname(os.path.abspath(__file__))
job_script_path = os.path.join(script_path, "shell_scripts")
shared_file_system = True
hostlist = []
screen_names = []
resubmit_cycle = 30.0
sub_delay = 0.0
save_log = True
log_suffix = ""

if (not job_script_path is None) and (not os.path.exists(job_script_path)):
    os.makedirs(job_script_path)

# list of environment contingencies
contingencies = []
for i, host in enumerate(hosts):
    args = host.split(';')
    if len(args) > 1:
        for j in range(len(args)):
            args[j] = args[j].strip()
        hosts[i] = args[0]
        args = [x for x in args[1:] if len(x) > 0]
        contingencies.append(args)
    else:
        hosts[i] = host.strip()
        contingencies.append([])

# list of start path for each host
dirs = []
for i, host in enumerate(hosts):
    if host in rundir and not rundir[host] is None and len(rundir[host]) > 0: 
        if type(rundir[host]) is str:
            dirs.append(rundir[host])
        else:
            if shared_file_system:
                dirs.append(script_path)
            else:
                dirs.append("~/")
            print "Warning: rundir[" + host + "] is not a string and is not empty!"
    else:
        if shared_file_system:
            dirs.append(script_path)
        else:
            dirs.append("~/")




def generate_job_script(script_content, filename, exec_command=None):
    global job_script_path

    # script_content should be a list where each entry is a line in
    # the script file; generally speaking, each entry corresponds to a
    # terminal command (bash, DOS, etc)
    if len(script_content) > 0:
        content = script_content[0] + "\n"
        for i in range(1, len(script_content)):
            content += script_content[1] + "\n"
    else:
        content = None
        print filename + " script empty; skipping."
        return None

    full_path = os.path.join(job_script_path, filename)
    with open(full_path, "w") as f:
        f.write(content)
    subprocess.call(['chmod', '+x', full_path])
        
    if exec_command is None:
        job = full_path
    else:
        job = exec_command

    return job

def random_name(prefix='rj', size=8, chars=string.ascii_uppercase + string.digits):
    return prefix + ''.join(random.choice(chars) for _ in range(size))

def unique_screen(this_screen_name, all_screen_names):
    if len(all_screen_names) > 0:
        return True
    else:
        for sn in all_screen_names:
            if this_screen_name == sn:
                return False
        return True

def generate_screen_name(all_screen_names):
    screen_name = random_name()
    while not unique_screen(screen_name, all_screen_names):
        screen_name = random_name()
    return screen_name

def screen_exists(screen_name):
    # this function is unused
    p = subprocess.Popen(['screen', '-ls'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    var, err = p.communicate()
    if "." + screen_name + "\t(" in var:
        print "True"
    else:
        print "False"    

def generate_cmd(host, screen_name, dir, contingency, job):
    global script_path
    global shared_file_system
    global envcom

    if shared_file_system:
        if len(dir) > 0:
            cmd = "cd " + dir + " && "
        else:
            cmd = "cd " + script_path + " && "
    else:
        if len(dir) > 0:
            cmd = "cd " + dir + " && "
        else:
            cmd = ""

    if len(contingency) > 0:
        for i, contingency in enumerate(contingency):
            if len(envcom[contingency]) < 1:
                continue
            else:
                cmd += envcom[contingency] + " && "

    cmd += "screen -dmS " + screen_name + " " + job

    return cmd

def verbose_cmd(host, screen_name, dir, contingency, job):
    global script_path
    global shared_file_system
    global envcom

    vb = []
    if dir == "~/":
        prmpt = username + "@" + host + ":~$ "
    else:
        vb.append(username + "@" + host + ":~$ cd " + dir)
        prmpt = username + "@" + host + ":" + dir + "$ "

    if len(contingency) > 0:
        for i in range(len(contingency)):
            vb.append(prmpt + envcom[contingency[i]])

    vb.append(prmpt + "screen -dmS " + screen_name + " " + job)
    vb.append(prmpt + "exit")    

    return vb

def write_log(hostlist, screen_names, dirs, contingencies):
    global script_path
    global log_suffix

    with open(script_path + "/rj" + log_suffix + ".log", "w") as f:
        for i in range(len(hostlist)):
            if len(contingencies[i]) > 0:
                f.write(hostlist[i] + ", " + screen_names[i] + ", " + dirs[i] + ", " + ", ".join(contingencies[i]) + "\n")
            else:
                f.write(hostlist[i] + ", " + screen_names[i] + ", " + dirs[i] + "\n")

def check_update_hosts():
    global script_path
    global log_suffix

    # check the log file for new/commented/removed entries
    with open(script_path + "/rj" + log_suffix + ".log", "r") as f:
        log = f.readlines()

    log = [l.split(',') for l in log]
    for i in range(len(log)):
        for j in range(len(log[i])-1):
            log[i][j] = log[i][j].strip()
        log[i][-1] = log[i][-1][:-1].strip()
    #log = [[h, id.strip()] for h, id in log]

    new_hostlist = []
    new_screen_names = []
    new_dirs = []
    new_contingencies = []
    for i in range(len(log)):
        if len(log[i][0]) > 0 and not log[i][0].startswith("#"):
            new_hostlist.append(log[i][0])
            if len(log[i]) > 1 and len(log[i][1]) > 0:
                new_screen_names.append(log[i][1])
            else:
                new_screen_name.append(generate_screen_names(zip(*log)[1]))
            if len(log[i]) > 2 and len(log[i][2]) > 0:
                new_dirs.append(log[i][2])
            elif shared_file_system:
                new_dirs.append(script_path)
            else:
                new_dirs.append("~/")
            if len(log[i]) > 3:
                contingency = []
                for j in range(3, len(log[i])):
                    if len(log[i][j]) > 0:
                        contingency.append(log[i][j])
                new_contingencies.append(contingency)
            else:
                new_contingencies.append([])      

    return (new_hostlist, new_screen_names, new_dirs, new_contingencies)



if not test:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    idx = 0
    for i, h in enumerate(hosts):
        try:
            client.connect(hostname=h, username=username, password=password)
            print "Connected to " + h
            screen_name = generate_screen_name(screen_names)
            screen_names.append(screen_name)
            cmd = generate_cmd(h, screen_name, dirs[i], contingencies[i], jobs[idx])
            vb = verbose_cmd(h, screen_name, dirs[i], contingencies[i], jobs[idx])
            for k in range(len(vb)-1):
                print vb[k]
            client.exec_command(cmd)
            idx += 1
            print vb[-1]
            print " "
            client.close()
            hostlist.append(h)
            time.sleep(sub_delay)
        except:
            print "Could not connect to " + h + ". Skipping..."
            print " "
            screen_name = generate_screen_name(screen_names)
            screen_names.append(screen_name)
            hostlist.append(h)

    if save_log:
        write_log(hostlist, screen_names, dirs, contingencies)

    if idx < len(jobs):
        print "There are more jobs than hosts! (njobs=" + str(len(jobs)) + ", nhosts=" + str(len(hosts)) + ")"
        print str(len(jobs)-idx) + " jobs remaining. Waiting for free hosts..."
    while idx < len(jobs):
        time.sleep(resubmit_cycle)
        new_submission = False

        if save_log:
            hostlist, screen_names, dirs, contingencies = check_update_hosts()

        for i, h in enumerate(hostlist):
            try:
                client.connect(hostname=h, username=username, password=password)
                stdin, stdout, stderr = client.exec_command("screen -ls; true")
                var = stdout.readlines()
                var = " ".join(var)
                if not "." + screen_names[i] + "\t(" in var:
                    print "Connected to " + h
                    cmd = generate_cmd(h, screen_name, dirs[i], contingencies[i], jobs[idx])
                    vb = verbose_cmd(h, screen_name, dirs[i], contingencies[i], jobs[idx])
                    for k in range(len(vb)-1):
                        print vb[k]
                    client.exec_command(cmd)
                    print vb[-1]
                    idx += 1
                    print " "
                    new_submission = True
                client.close()
                time.sleep(sub_delay)
                if idx >= len(jobs):
                    break
            except:
                print "Error: could not connect to host " + h + ". Skipping this cycle..."
        if new_submission and idx < len(jobs):
            print str(len(jobs)-idx) + " jobs remaining. Waiting for free hosts..."

    print "Job submission complete. Exiting..."

else:
    if len(hosts) < len(jobs):
        for idx, h in enumerate(hosts):
            print "Connected to " + h
            screen_name = generate_screen_name(screen_names)
            screen_names.append(screen_name)
            vb = verbose_cmd(h, screen_name, dirs[idx], contingencies[idx], jobs[idx])
            for k in range(len(vb)):
                print vb[k]
            print " "

        print "There are more jobs than hosts! (njobs=" + str(len(jobs)) + ", nhosts=" + str(len(hosts)) + ")"
    else:
        for idx, job in enumerate(jobs):
            print "Connected to " + hosts[idx]
            screen_name = generate_screen_name(screen_names)
            screen_names.append(screen_name)
            vb = verbose_cmd(hosts[idx], screen_name, dirs[idx], contingencies[idx], job)
            for k in range(len(vb)):
                print vb[k]
            print " "

    print "Test output complete. Exiting..."
