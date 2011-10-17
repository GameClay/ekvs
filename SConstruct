import os, sys, platform

env = Environment()

# Hax
architecture="x86"
variant = architecture+'/Debug'

# Absolute path for includes and libs
env['EKVS_INCLUDE'] = [os.path.abspath('include/')]
env['EKVS_LIB'] = [os.path.abspath('lib/'+variant)]

env['CCFLAGS'] += ['-std=c89','-Werror','-pedantic','-ggdb']

libekvs = SConscript('src/SConscript', variant_dir='lib/'+variant, duplicate=False, exports='env')
test = SConscript('test/SConscript', variant_dir='bin/'+variant, duplicate=False, exports='env')
