import os
import subprocess

print("GIT STATUS:")
print(subprocess.getoutput("git status"))
print("\nGIT DIFF:")
print(subprocess.getoutput("git diff"))
print("\nGIT LOG -1:")
print(subprocess.getoutput("git log -1"))
