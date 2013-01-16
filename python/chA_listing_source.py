
'''
# <start id="linux-redis-install"/>
~:$ wget -q http://redis.googlecode.com/files/redis-2.6.2.tar.gz  #A
~:$ tar -xzf redis-2.6.2.tar.gz                            #B
~:$ cd redis-2.6.2/
~/redis-2.6.2:$ make                                    #C
cd src && make all                                          #D
[trimmed]                                                   #D
make[1]: Leaving directory `~/redis-2.6.2/src'          #D
~/redis-2.6.2:$ sudo make install                       #E
cd src && make install                                      #F
[trimmed]                                                   #F
make[1]: Leaving directory `~/redis-2.6.2/src'          #F
~/redis-2.6.2:$ redis-server redis.conf                             #G
[13792] 26 Aug 17:53:16.523 * Max number of open files set to 10032 #H
[trimmed]                                                           #H
[13792] 26 Aug 17:53:16.529 * The server is now ready to accept     #H
connections on port 6379                                            #H
# <end id="linux-redis-install"/>
#A Download the most recent version of Redis 2.6 (we use some features of Redis 2.6 in other chapters, but you can use the most recent version you are comfortable with by finding the download link: http://redis.io/download )
#B Extract the source code
#C Compile Redis
#D Watch compilation messages go by, you shouldn't see any errors
#E Install Redis
#F Watch installation messages go by, you shouldn't see any errors
#G Start Redis server
#H See the confirmation that Redis has started
#END
'''

'''
# <start id="linux-python-install"/>
~:$ wget -q http://peak.telecommunity.com/dist/ez_setup.py          #A
~:$ sudo python ez_setup.py                                         #B
Downloading http://pypi.python.org/packages/2.7/s/setuptools/...    #B
[trimmed]                                                           #B
Finished processing dependencies for setuptools==0.6c11             #B
~:$ sudo python -m easy_install redis hiredis                       #C
Searching for redis                                                 #D
[trimmed]                                                           #D
Finished processing dependencies for redis                          #D
Searching for hiredis                                               #E
[trimmed]                                                           #E
Finished processing dependencies for hiredis                        #E
~:$
# <end id="linux-python-install"/>
#A Download the setuptools ez_setup module
#B Run the ez_setup module to download and install setuptools
#C Run setuptools' easy_install package to install the redis and hiredis packages
#D The redis package offers a somewhat standard interface to Redis from Python
#E The hiredis package is a C accelerator library for the Python Redis library
#END
'''

'''
# <start id="mac-redis-install"/>
~:$ curl -O http://rudix.googlecode.com/hg/Ports/rudix/rudix.py     #A
[trimmed]
~:$ sudo python rudix.py install rudix                              #B
Downloading rudix.googlecode.com/files/rudix-12.6-0.pkg             #C
[trimmed]                                                           #C
installer: The install was successful.                              #C
All done                                                            #C
~:$ sudo rudix install redis                                        #D
Downloading rudix.googlecode.com/files/redis-2.4.15-0.pkg           #E
[trimmed]                                                           #E
installer: The install was successful.                              #E
All done                                                            #E
~:$ redis-server                                                    #F
[699] 13 Jul 21:18:09 # Warning: no config file specified, using the#G
default config. In order to specify a config file use 'redis-server #G
/path/to/redis.conf'                                                #G
[699] 13 Jul 21:18:09 * Server started, Redis version 2.4.15        #G
[699] 13 Jul 21:18:09 * The server is now ready to accept connections#G
on port 6379                                                        #G
[699] 13 Jul 21:18:09 - 0 clients connected (0 slaves), 922304 bytes#G
in use                                                              #G
# <end id="mac-redis-install"/>
#A Download the bootstrap script that installs Rudix
#B Tell Rudix to install itself
#C Rudix is downloading and installing itself
#D Tell Rudix to install Redis
#E Rudix is downloading and installing Redis - note that we use some features from Redis 2.6, which is not yet available from Rudix
#F Start the Redis server
#G Redis started, and is running with the default configuration
#END
'''

'''
# <start id="mac-python-install"/>
~:$ sudo rudix install pip                              #A
Downloading rudix.googlecode.com/files/pip-1.1-1.pkg    #B
[trimmed]                                               #B
installer: The install was successful.                  #B
All done                                                #B
~:$ sudo pip install redis                              #C
Downloading/unpacking redis                             #D
[trimmed]                                               #D
Cleaning up...                                          #D
~:$
# <end id="mac-python-install"/>
#A Because we have Rudix installed, we can install a Python package manager called pip
#B Rudix is installing pip
#C We can now use pip to install the Python Redis client library
#D Pip is installing the Redis client library for Python
#END
'''

'''
# <start id="windows-python-install"/>
C:\Users\josiah>c:\python27\python                                      #A
Python 2.7.3 (default, Apr 10 2012, 23:31:26) [MSC v.1500 32 bit...
Type "help", "copyright", "credits" or "license" for more information.
>>> from urllib import urlopen                                          #B
>>> data = urlopen('http://peak.telecommunity.com/dist/ez_setup.py')    #C
>>> open('ez_setup.py', 'wb').write(data.read())                        #D
>>> exit()                                                              #E

C:\Users\josiah>c:\python27\python ez_setup.py                          #F
Downloading http://pypi.python.org/packages/2.7/s/setuptools/...        #G
[trimmed]                                                               #G
Finished processing dependencies for setuptools==0.6c11                 #G

C:\Users\josiah>c:\python27\python -m easy_install redis                #H
Searching for redis                                                     #H
[trimmed]                                                               #H
Finished processing dependencies for redis                              #H
C:\Users\josiah>
# <end id="windows-python-install"/>
#A Start Python by itself in interactive mode
#B Import the urlopen factory function from the urllib module
#C Fetch a module that will help us install other packages
#D Write the downloaded module to a file on disk
#E Quit the Python interpreter by running the builtin exit() function
#F Run the ez_setup helper module
#G The ez_setup helper downloads and installs setuptools, which will make it easy to download and install the Redis client library
#H Use setuptools' easy_install module to download and install Redis
#END
'''


'''
# <start id="hello-redis-appendix"/>
~:$ python                                          #A
Python 2.6.5 (r265:79063, Apr 16 2010, 13:09:56) 
[GCC 4.4.3] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>> import redis                                    #B
>>> conn = redis.Redis()                            #C
>>> conn.set('hello', 'world')                      #D
True                                                #D
>>> conn.get('hello')                               #E
'world'                                             #E
# <end id="hello-redis-appendix"/>
#A Start Python so that we can verify everything is up and running correctly
#B Import the redis library, it will automatically use the hiredis C accelerator library if it is available
#C Create a connection to Redis
#D Set a value and see that it was set
#E Get the value we just set
#END
'''
