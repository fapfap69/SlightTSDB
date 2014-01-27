/*
 * TSAccess.h
 *
 *  Created on: 27/nov/2013
 *      Author: fap
 */

#ifndef TSACCESS_H_
#define TSACCESS_H_

/* Key flags */
#define TSM_KEY_IN_USE			(1 << 0)

/* Length of HMAC keys (bytes).  Only database key management is handled here as
 * part of the metadata.  API layers are expected to handle the actual access control
 * process in a way that is appropriate for the particular API */


#define TSM_PERSISTENT_ADMINKEY 1
#define TSM_VOLATILE_ADMINKEY 2

#include "cJSON/cJSON.h"

// Function prototype
void tsaGenAdminKey(int isPersistent, tsm_key_t *gAdminKey);


#endif /* TSACCESS_H_ */
