# import subprocess
#
# print('$ nslookup www.python.org')
# r = subprocess.call(['nslookup', 'www.python.org'])
# print('Exit code:', r)

# import subprocess

# print('$ nslookup')
# p = subprocess.Popen(['nslookup'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# output, err = p.communicate(b'set q=mx\npython.org\nexit\n')
# print(output.decode('utf-8'))
# print('Exit code:', p.returncode)


import subprocess

# ok
pipe = subprocess.Popen( 'ls /bin', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
# res = tuple (stdout, stderr)
res = pipe.communicate()
if pipe.returncode != 0:
    print("stderr =", res[1])