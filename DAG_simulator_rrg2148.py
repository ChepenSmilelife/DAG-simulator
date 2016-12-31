# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 00:46:31 2016

AUTHOR: Ralston R. Goes
UNI:    rrg2148
COURSE: ELEN E4904 Mobile Cloud: Project
TITLE:  Task Scheduling Simulator for DAGs using Greedy Algorithm

"""

import math
import timeit
# This class is for a task represented as a node object
class dag_node:
    
    def __init__(self):
        self.predecessors = []
        self.completed = False
        self.started = False
        self.arrival_time = math.inf
        self.can_start = False
        self.cloud_timings = []
        self.cloud_chosen = math.inf
        
    def check_started(self):
        return self.started
    
    def check_completed(self):
        return self.completed
        
    def set_started(self, started):
        self.started = started

    def set_completed(self, completed):
        self.completed = completed
    
    def set_cloud(self, cloud_chosen):
        self.cloud_chosen = cloud_chosen
    
    def get_cloud_timings(self):
        return self.cloud_timings


# This class is for a cloud server object 
class cloud:
    
    def __init__(self):
        self.busy = False
        
    def set_busy(self, busy):
        self.busy = busy

    def check_busy(self):
        return self.busy


#Takes number of tasks and cloud servers as input
num_tasks = int(input("Enter number of tasks: "))
num_clouds = int(input("Enter number of cloud servers: "))

# Create objects for tasks and cloud servers
tasks = [dag_node() for i in range(num_tasks)]
clouds = [cloud() for i in range(num_clouds)]

          
# Define the tasks and clouds ------------------------------------------------------ 
for i in range(num_tasks):
    num_pred=0
    print("\n------------------------\nEnter details of Task %d: " % (i))
    for j in range(num_clouds):
        exec_time = int(input("Enter the task's execution time in cloud %d: " % (j)))
        tasks[i].cloud_timings.append((exec_time,j))
    ans = str(input("Does the task have a pre-defined arrival time?(y/n) "))
    if ans=="y":
        tasks[i].arrival_time = int(input("Enter the arrival time: "))
    else:
        num_pred = int(input("How many predecessors does the task have? "))
    while num_pred!=0:
        pred_task = int(input("Enter predecessor's task number: "))
        tasks[i].predecessors.append(pred_task)
        num_pred -= 1


# Simulating the DAG ---------------------------------------------------------------
t=-1    # Clock
in_process = []    # This list stores the tasks currently in process
num=0
print("")
program_start = timeit.default_timer()

while num < num_tasks:    
    
    t=t+1
    
    # This sub-code processes the tasks in in_process[]
    if in_process:
        in_process = [(x,y-1,z) for x,y,z in in_process]
        for m in range(len(in_process)):
            if in_process[m][1] == 0:
                task_num = in_process[m][0]
                cloud_num = in_process[m][2]
                tasks[task_num].set_completed(True)
                clouds[cloud_num].set_busy(False)
                num += 1
                print("Task %d completed at %d using cloud %d" % (task_num,t,cloud_num))
        in_process = [(x,y,z) for x,y,z in in_process if y>0]
                      
    if num == num_tasks:
        break
    
    # This sub-code checks and updates tasks if they are ready to start   
    for i in range(num_tasks):
        if tasks[i].arrival_time == t:
            tasks[i].can_start = True
        elif len(tasks[i].predecessors)>0 and tasks[i].can_start == False:
            check = 0
            for k in range(len(tasks[i].predecessors)):
                if tasks[tasks[i].predecessors[k]].completed == True:
                    check += 1
            if check == len(tasks[i].predecessors):
                tasks[i].can_start = True

    # This sub-code assigns tasks that can start to cloud servers using greedy
    for i in range(num_tasks):
        found = False
        if tasks[i].can_start == True and tasks[i].started==False:
            clds = tasks[i].get_cloud_timings()
            while found == False and clds:
                min_val = math.inf
                for j in range(len(clds)):
                    if min_val > clds[j][0]:
                        min_val = clds[j][0]
                        min_val_idx = clds[j][1]
                        min_val_pos = j
                if clouds[min_val_idx].check_busy() == False:
                    found = True
                    tasks[i].set_started(True)
                    tasks[i].set_cloud(min_val_idx)
                    clouds[min_val_idx].set_busy(True)
                    in_process.append((i,min_val,min_val_idx))
                else:
                    clds.pop(min_val_pos)
        
program_end = timeit.default_timer()
               
print("The optimal time using greedy is: %d" % (t))
print("The optimal result cost is: %f ms" % ((program_end - program_start)*(1000.0)))

            