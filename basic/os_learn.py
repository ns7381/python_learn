import os
print(os.name)
print(os.uname())
print(os.environ)
print(os.environ.get("PATH"))
import subprocess, os
my_env = os.environ

print("-"*100)
sss = '''
`(dt|rank)?+.+`'"\\['
'''
print(sss.replace('\\', '\\\\\\\\\\').replace('"', "\\\\\\\"").replace("`", "\\`"))
print(sss)
PROJECT_ABSOLUTE_PATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print('PROJECT_ABSOLUTE_PATH:%s' % PROJECT_ABSOLUTE_PATH)