#include <ekvs/ekvs.h>
#include "../src/ekvs_internal.h"

#include <cspec.h>
#include <cspec_output_verbose.h>

DEFINE_DESCRIPTION(ekvs_open)
DEFINE_DESCRIPTION(ekvs_set_ex)
DEFINE_DESCRIPTION(ekvs_get)
DEFINE_DESCRIPTION(ekvs_del)
DEFINE_DESCRIPTION(ekvs_snapshot)

int main()
{
   CSpec_Run(DESCRIPTION(ekvs_open), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_set_ex), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_get), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_del), CSpec_NewOutputVerbose());
   CSpec_Run(DESCRIPTION(ekvs_snapshot), CSpec_NewOutputVerbose());
   return 0;
}
