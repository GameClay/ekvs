import os, sys, platform

env = Environment()

# Hax
architecture="x86"
variant = architecture+'/Debug'

# Absolute path for include
env['EKVS_INCLUDE'] = [os.path.abspath('include/')]

libekvs = SConscript('src/SConscript', variant_dir='lib/'+variant, duplicate=False, exports='env')
