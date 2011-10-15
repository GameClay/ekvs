import os, sys, platform

env = Environment()

# Hax
architecture="x86"
variant = architecture+'/Debug'

libekvs = SConscript('src/SConscript', variant_dir='lib/'+variant, duplicate=False, exports='env')
