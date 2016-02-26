import sys
import subprocess

def runcommand(cmd,ignore_err=False,exit_on_error=True):
     p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
     (out,err) = p.communicate()
     if not ignore_err:
         if exit_on_error and err!="":
             print err
             sys.exit()
         return (out,err)
     return out

