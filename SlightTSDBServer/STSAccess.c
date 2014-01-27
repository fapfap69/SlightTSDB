/*
 * TSAccess.c
 *
 *  Created on: 27/nov/2013
 *      Author: fap
 */


// external packages

//#include "TSHttpd.h"
#include "profile.h"
#include "base64.h"

// Project specific includes
#include "SlightTSDB.h"
#include "STSCore.h"
#include "STSLog.h"

#include "STSAccess.h"

// key signature check
#define ADMIN_KEY_FILE		"adminkey.txt"

// Generate the Administrative Key
//
void tsaGenAdminKey(int isPersistent, tsm_key_t *gAdminKey) {

	const char *keychars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890^(){}[]-_=+;:@#~<>,./?";
	char *ptr = (char *)gAdminKey;
	time_t now;
	int n, nkeychars;
	FILE *keyfile;

	FUNCTION_TRACE;

	now = time(NULL);
	srandom(now);

	// ask for persistent this read the key from file
	if (isPersistent == TSM_PERSISTENT_ADMINKEY) {
		int nread = 0;
		keyfile = fopen(ADMIN_KEY_FILE, "r");
		if (keyfile > 0) {
			nread = fread(gAdminKey, 1, sizeof(tsm_key_t), keyfile);
			fclose(keyfile);
		}
		if (nread == sizeof(tsm_key_t)) {
			DEBUG("STSAccess :: Read persistent admin key: %.*s\n", (int)sizeof(tsm_key_t), (char*)gAdminKey);
			return;
		}
	}
	// or ask for new generation of key
	if (isPersistent == TSM_VOLATILE_ADMINKEY) {
		nkeychars = strlen(keychars);
		for (n = 0; n < sizeof(tsm_key_t); n++) {
			*ptr++ = keychars[random() % nkeychars];
		}

		keyfile = fopen(ADMIN_KEY_FILE, "w");
		if (keyfile < 0) {
			ERROR("STSAccess :: Failed opening Key file\n");
			return;
		}
		fprintf(keyfile, "%.*s\n", (int)sizeof(tsm_key_t), (char*)gAdminKey);
		fclose(keyfile);
	}
	return;
}

